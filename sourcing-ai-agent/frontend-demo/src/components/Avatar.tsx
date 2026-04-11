import { useMemo, useState } from "react";

interface AvatarProps {
  name: string;
  src?: string;
  size?: "small" | "large";
}

function pickInitial(name: string): string {
  const trimmed = (name || "").trim();
  if (!trimmed) {
    return "U";
  }
  const first = trimmed[0];
  return first.toUpperCase();
}

export function Avatar({ name, src, size = "small" }: AvatarProps) {
  const [hasError, setHasError] = useState(false);
  const initial = useMemo(() => pickInitial(name), [name]);
  const showImage = !!src && !hasError;
  const className = size === "large" ? "hero-avatar" : "avatar";

  if (showImage) {
    return <img className={className} src={src} alt={name} onError={() => setHasError(true)} />;
  }

  return (
    <div className={`${className} avatar-fallback`} aria-label={name}>
      <span>{initial}</span>
    </div>
  );
}

