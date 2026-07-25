import { useSearchParams } from "react-router-dom";
import { LocalAssetTabs } from "../components/LocalAssetTabs";
import { TargetCandidatesPanel } from "../components/TargetCandidatesPanel";

export function TargetCandidatesPage() {
  const [searchParams] = useSearchParams();
  const collectionId = (searchParams.get("collection") || "").trim();

  return (
    <section className="page local-asset-target-page">
      <LocalAssetTabs active="targets" collectionId={collectionId} />
      <TargetCandidatesPanel sourceCollectionId={collectionId} />
    </section>
  );
}
