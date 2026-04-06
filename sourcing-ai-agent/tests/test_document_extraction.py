import unittest

from sourcing_agent.document_extraction import (
    build_candidate_patch_from_analysis,
    build_page_analysis_input,
    detect_document_type,
    extract_pdf_text,
    google_doc_export_url,
)
from sourcing_agent.domain import Candidate


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


if __name__ == "__main__":
    unittest.main()
