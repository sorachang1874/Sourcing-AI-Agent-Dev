import { useEffect, useMemo, useState } from "react";
import { pickSurnameInitial } from "../lib/candidatePresentation";

interface AvatarProps {
  name: string;
  src?: string;
  size?: "small" | "large";
}

export function Avatar({ name, src, size = "small" }: AvatarProps) {
  const [hasError, setHasError] = useState(false);
  const initial = useMemo(() => pickSurnameInitial(name), [name]);
  const normalizedSrc = useMemo(() => {
    const trimmed = (src || "").trim();
    if (!trimmed) {
      return "";
    }
    if (trimmed.startsWith("//")) {
      return `https:${trimmed}`;
    }
    return trimmed;
  }, [src]);
  const showImage = !!normalizedSrc && !hasError;
  const className = size === "large" ? "hero-avatar" : "avatar";

  useEffect(() => {
    setHasError(false);
  }, [normalizedSrc]);

  if (showImage) {
    return (
      <img
        className={className}
        src={normalizedSrc}
        alt={name}
        loading="lazy"
        decoding="async"
        referrerPolicy="no-referrer"
        onError={() => setHasError(true)}
      />
    );
  }

  return (
    <div className={`${className} avatar-fallback`} aria-label={name}>
      <span>{initial}</span>
    </div>
  );
}
