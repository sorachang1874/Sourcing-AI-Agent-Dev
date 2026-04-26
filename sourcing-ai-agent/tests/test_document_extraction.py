import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from sourcing_agent.asset_logger import AssetLogger
from sourcing_agent.document_extraction import (
    analyze_remote_document,
    build_candidate_patch_from_analysis,
    build_document_evidence_slice,
    build_page_analysis_input,
    detect_document_type,
    empty_signal_bundle,
    extract_page_signals,
    extract_pdf_front_matter_text,
    extract_pdf_text,
    google_doc_export_url,
    infer_pdf_document_type,
    normalize_analysis,
)
from sourcing_agent.domain import Candidate
from sourcing_agent.web_fetch import FetchedTextAsset


def _build_simple_pdf(text_lines: list[str]) -> bytes:
    commands = ["BT", "/F1 12 Tf", "72 720 Td"]
    first = True
    for line in text_lines:
        if not first:
            commands.append("0 -16 Td")
        escaped = line.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
        commands.append(f"({escaped}) Tj")
        first = False
    commands.append("ET")
    stream = "\n".join(commands).encode("utf-8")
    objects = [
        b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        b"2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        (
            b"3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
            b"/Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n"
        ),
        b"4 0 obj\n<< /Length " + str(len(stream)).encode("ascii") + b" >>\nstream\n" + stream + b"\nendstream\nendobj\n",
        b"5 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>\nendobj\n",
    ]
    output = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for obj in objects:
        offsets.append(len(output))
        output.extend(obj)
    xref_start = len(output)
    output.extend(f"xref\n0 {len(objects)+1}\n".encode("ascii"))
    output.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        output.extend(f"{offset:010d} 00000 n \n".encode("ascii"))
    output.extend(
        f"trailer\n<< /Size {len(objects)+1} /Root 1 0 R >>\nstartxref\n{xref_start}\n%%EOF\n".encode("ascii")
    )
    return bytes(output)


class DocumentExtractionTest(unittest.TestCase):
    def test_google_doc_export_url(self) -> None:
        self.assertEqual(
            google_doc_export_url(
                "https://docs.google.com/document/d/1yGn5ZDFaAj7tI2-0ZyMEASX1uvOjwld2aHeE1V8Mzg0/edit?tab=t.0"
            ),
            "https://docs.google.com/document/d/1yGn5ZDFaAj7tI2-0ZyMEASX1uvOjwld2aHeE1V8Mzg0/export?format=txt",
        )

    def test_detect_document_type(self) -> None:
        self.assertEqual(
            detect_document_type(source_url="https://jeremybernste.in/cv/", title="Jeremy Bernstein CV"),
            "html_resume",
        )
        self.assertEqual(
            detect_document_type(source_url="https://horace.io/files/horace.pdf", title="Horace Resume"),
            "pdf_resume",
        )

    def test_build_page_analysis_input_includes_document_metadata(self) -> None:
        candidate = Candidate(candidate_id="c1", name_en="Jane Doe", display_name="Jane Doe")
        html = """
        <html>
          <head><title>Jane Doe CV</title><meta name="description" content="Research engineer at Thinking Machines Lab."></head>
          <body>
            <h1>Experience</h1>
            <p>Research Engineer at Thinking Machines Lab</p>
            <h1>Education</h1>
            <p>MIT, PhD in Computer Science</p>
          </body>
        </html>
        """
        payload = build_page_analysis_input(
            candidate=candidate,
            target_company="Thinking Machines Lab",
            source_url="https://janedoe.dev/cv/",
            html_text=html,
            extracted_links={"linkedin_urls": [], "x_urls": [], "github_urls": [], "personal_urls": [], "resume_urls": []},
        )
        self.assertEqual(payload["document_type"], "html_resume")
        self.assertTrue(payload["text_blocks"])
        self.assertIn("Experience", "\n".join(payload["text_blocks"]))

    def test_github_evidence_slice_keeps_profile_contact_block(self) -> None:
        candidate = Candidate(candidate_id="c1", name_en="Noah Yonack", display_name="Noah Yonack")
        html = """
        <html><head><title>Noah Yonack (noahyonack)</title></head>
        <body>
          <main>
            <div>Noah Yonack</div>
            <div>noahyonack</div>
            <div>Data Scientist. Harvard '17.</div>
            <div>San Francisco, CA</div>
            <div>noah.yonack@gmail.com</div>
          </main>
        </body></html>
        """
        payload = build_page_analysis_input(
            candidate=candidate,
            target_company="Perplexity",
            source_url="https://github.com/noahyonack",
            html_text=html,
            extracted_links=empty_signal_bundle(),
        )
        evidence_slice = build_document_evidence_slice(
            analysis_input=payload,
            signals=empty_signal_bundle(),
            source_url="https://github.com/noahyonack",
            final_url="https://github.com/noahyonack",
            source_text=html,
        )

        self.assertEqual(evidence_slice["source_type"], "github_profile")
        self.assertIn("Noah Yonack", evidence_slice["selected_text"])
        self.assertIn("Data Scientist. Harvard '17.", evidence_slice["selected_text"])
        self.assertIn("noah.yonack@gmail.com", evidence_slice["selected_text"])
        self.assertEqual(evidence_slice["email_contexts"][0]["email"], "noah.yonack@gmail.com")

    def test_scholar_evidence_slice_keeps_academic_profile_signals(self) -> None:
        candidate = Candidate(candidate_id="c1", name_en="Noam Shazeer", display_name="Noam Shazeer")
        html = """
        <html><head><title>Noam Shazeer - Google Scholar</title></head>
        <body>
          <div id="gsc_prf_i">
            <div id="gsc_prf_in">Noam Shazeer</div>
            <div class="gsc_prf_il"><a class="gsc_prf_ila">Google</a></div>
            <div class="gsc_prf_il" id="gsc_prf_ivh">
              Verified email at google.com -
              <a href="https://www.noamshazeer.com/" class="gsc_prf_ila">Homepage</a>
            </div>
            <a class="gsc_prf_inta">Deep Learning</a>
            <a class="gsc_prf_inta">Natural Language Processing</a>
          </div></div></div><div id="gsc_prf_t_wrp"></div>
          <table id="gsc_rsb_st">
            <tr><td class="gsc_rsb_sc1">Citations</td><td class="gsc_rsb_std">250000</td><td class="gsc_rsb_std">200000</td></tr>
            <tr><td class="gsc_rsb_sc1">h-index</td><td class="gsc_rsb_std">80</td><td class="gsc_rsb_std">70</td></tr>
          </table>
          <table>
            <tr class="gsc_a_tr">
              <td class="gsc_a_t">
                <a href="/citations?view_op=view_citation&citation_for_view=abc" class="gsc_a_at">Attention Is All You Need</a>
                <div class="gs_gray">A Vaswani, N Shazeer, N Parmar</div>
                <div class="gs_gray">NeurIPS</div>
              </td>
              <td class="gsc_a_c"><a class="gsc_a_ac">100000</a></td>
              <td class="gsc_a_y"><span class="gsc_a_h">2017</span></td>
            </tr>
          </table>
        </body></html>
        """
        source_url = "https://scholar.google.com/citations?user=abc&hl=en"
        signals = extract_page_signals(html, source_url)
        payload = build_page_analysis_input(
            candidate=candidate,
            target_company="Google",
            source_url=source_url,
            html_text=html,
            extracted_links=signals,
        )
        evidence_slice = build_document_evidence_slice(
            analysis_input=payload,
            signals=signals,
            source_url=source_url,
            final_url=source_url,
            source_text=html,
        )

        self.assertEqual(evidence_slice["source_type"], "google_scholar_profile")
        self.assertEqual(signals["scholar_verified_domains"], ["google.com"])
        self.assertIn("Deep Learning", signals["research_interests"])
        self.assertEqual(signals["scholar_citation_metrics"][0]["citations"], "250000")
        self.assertEqual(signals["scholar_publications"][0]["title"], "Attention Is All You Need")
        self.assertEqual(evidence_slice["structured_signals"]["scholar_publications"][0]["year"], "2017")

    def test_page_signals_do_not_treat_media_assets_as_personal_urls(self) -> None:
        html = """
        <html><body>
          <a href="/blog">Blog</a>
          <a href="/assets/demo.gif">Demo gif</a>
          <a href="/files/cv.pdf">CV</a>
          <a href="https://github.com/login?return_to=%2Ftxie-93%2Fcdvae">GitHub login</a>
          <a href="https://github.com/txie-93/cdvae">GitHub repo</a>
        </body></html>
        """
        signals = extract_page_signals(html, "https://xiangfu.co/")

        self.assertIn("https://xiangfu.co/blog", signals["personal_urls"])
        self.assertNotIn("https://xiangfu.co/assets/demo.gif", signals["personal_urls"])
        self.assertNotIn("https://xiangfu.co/files/cv.pdf", signals["personal_urls"])
        self.assertIn("https://xiangfu.co/files/cv.pdf", signals["resume_urls"])
        self.assertNotIn("https://github.com/login?return_to=%2Ftxie-93%2Fcdvae", signals["github_urls"])
        self.assertIn("https://github.com/txie-93/cdvae", signals["github_urls"])

    def test_analyze_remote_document_can_skip_model_analysis_and_write_evidence_slice(self) -> None:
        candidate = Candidate(candidate_id="c1", name_en="Noah Yonack", display_name="Noah Yonack")

        class ExplodingPageModel:
            calls = 0

            def analyze_page_asset(self, payload: dict) -> dict:
                del payload
                self.calls += 1
                raise AssertionError("per-document model analysis should be disabled")

        model = ExplodingPageModel()
        html = "<html><head><title>Noah Yonack</title></head><body>Noah Yonack noah.yonack@gmail.com</body></html>"
        with tempfile.TemporaryDirectory() as tempdir:
            logger = AssetLogger(tempdir)
            with patch(
                "sourcing_agent.document_extraction.fetch_text_url",
                return_value=FetchedTextAsset(
                    url="https://github.com/noahyonack",
                    final_url="https://github.com/noahyonack",
                    content_type="text/html",
                    text=html,
                    source_label="test",
                ),
            ):
                analyzed = analyze_remote_document(
                    candidate=candidate,
                    target_company="Perplexity",
                    source_url="https://github.com/noahyonack",
                    asset_dir=Path(tempdir) / "documents",
                    asset_logger=logger,
                    model_client=model,  # type: ignore[arg-type]
                    source_kind="test",
                    asset_prefix="doc_01_github",
                    analyze_with_model=False,
                )

        self.assertEqual(model.calls, 0)
        self.assertTrue(analyzed.evidence_slice_path.endswith("_evidence_slice.json"))
        self.assertIn("noah.yonack@gmail.com", analyzed.evidence_slice["selected_text"])
        self.assertEqual(analyzed.analysis["notes"], "Deterministic page analysis fallback.")

    def test_extract_pdf_text(self) -> None:
        pdf_bytes = _build_simple_pdf(
            [
                "John Doe Resume",
                "Experience: Research Engineer at Thinking Machines Lab",
                "Education: MIT PhD in Computer Science",
            ]
        )
        text = extract_pdf_text(pdf_bytes)
        self.assertIn("John Doe Resume", text)
        self.assertIn("Thinking Machines Lab", text)

    def test_extract_pdf_front_matter_uses_bounded_front_text(self) -> None:
        front_matter = extract_pdf_front_matter_text(
            b"not-a-real-pdf",
            fallback_text="Noam Shazeer Google noam@google.com\n" + ("late-page-noise " * 100),
            max_chars=80,
        )

        self.assertIn("noam@google.com", front_matter)
        self.assertNotIn("late-page-noise late-page-noise late-page-noise late-page-noise late-page-noise", front_matter)

    def test_infer_pdf_document_type_uses_public_web_hint_without_promoting_publications_to_resume(self) -> None:
        self.assertEqual(
            infer_pdf_document_type(
                source_url="https://aclanthology.org/2020.emnlp-main.437.pdf",
                hint="publication_url",
            ),
            "pdf_publication",
        )
        self.assertEqual(
            infer_pdf_document_type(
                source_url="https://example.edu/files/resume.pdf",
                hint="",
            ),
            "pdf_resume",
        )

    def test_build_candidate_patch_from_analysis_uses_structured_signals(self) -> None:
        candidate = Candidate(candidate_id="c1", name_en="Horace He", display_name="Horace He", category="lead")
        analysis = {
            "document_type": "pdf_resume",
            "role_signals": ["research", "compiler"],
            "education_signals": [{"school": "MIT", "degree": "BS", "field": "Computer Science", "date_range": "2015-2019"}],
            "work_history_signals": [
                {"title": "Research Engineer", "organization": "Thinking Machines Lab", "date_range": "2025-Present"},
                {"title": "Researcher", "organization": "OpenAI", "date_range": "2023-2025"},
            ],
            "affiliation_signals": [{"organization": "Thinking Machines Lab", "relation": "explicit_current_affiliation", "evidence": "Research Engineer at Thinking Machines Lab"}],
            "recommended_links": {"resume_url": "https://horace.io/files/horace.pdf"},
        }
        patch = build_candidate_patch_from_analysis(candidate, analysis, target_company="Thinking Machines Lab", source_url="https://horace.io/files/horace.pdf")
        self.assertIn("MIT", patch["education"])
        self.assertIn("Thinking Machines Lab", patch["work_history"])
        self.assertEqual(patch["organization"], "Thinking Machines Lab")
        self.assertEqual(patch["media_url"], "https://horace.io/files/horace.pdf")

    def test_normalize_analysis_tolerates_non_dict_recommended_links(self) -> None:
        normalized = normalize_analysis(
            {
                "summary": "Model returned malformed recommended_links.",
                "recommended_links": "https://example.com/cv.pdf",
            }
        )

        self.assertEqual(normalized["recommended_links"]["resume_url"], "")
        self.assertEqual(normalized["summary"], "Model returned malformed recommended_links.")


if __name__ == "__main__":
    unittest.main()
