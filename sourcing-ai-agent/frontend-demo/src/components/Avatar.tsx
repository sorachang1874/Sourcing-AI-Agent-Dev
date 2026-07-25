import { useEffect, useMemo, useState } from "react";
import { pickSurnameInitial } from "../lib/candidatePresentation";

interface AvatarProps {
  name: string;
  src?: string;
  size?: "small" | "large";
}

const avatarPalettes = [
  { background: "linear-gradient(135deg, #fee2d5 0%, #f97316 100%)", color: "#7c2d12" },
  { background: "linear-gradient(135deg, #d7f0ff 0%, #0ea5e9 100%)", color: "#0c4a6e" },
  { background: "linear-gradient(135deg, #dcfce7 0%, #22c55e 100%)", color: "#14532d" },
  { background: "linear-gradient(135deg, #fef3c7 0%, #f59e0b 100%)", color: "#78350f" },
  { background: "linear-gradient(135deg, #fae8ff 0%, #d946ef 100%)", color: "#701a75" },
  { background: "linear-gradient(135deg, #e0e7ff 0%, #6366f1 100%)", color: "#312e81" },
];

function hashText(value: string): number {
  let hash = 0;
  for (let index = 0; index < value.length; index += 1) {
    hash = (hash * 31 + value.charCodeAt(index)) >>> 0;
  }
  return hash;
}

function isExpiredLinkedInMediaUrl(src: string): boolean {
  try {
    const url = new URL(src);
    if (!url.hostname.endsWith("media.licdn.com")) {
      return false;
    }
    const expiresAtSeconds = Number(url.searchParams.get("e") || 0);
    return Number.isFinite(expiresAtSeconds) && expiresAtSeconds > 0 && expiresAtSeconds * 1000 < Date.now();
  } catch {
    return false;
  }
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
  const providerImageExpired = useMemo(() => isExpiredLinkedInMediaUrl(normalizedSrc), [normalizedSrc]);
  const showImage = !!normalizedSrc && !hasError && !providerImageExpired;
  const className = size === "large" ? "hero-avatar" : "avatar";
  const fallbackStyle = useMemo(() => {
    const palette = avatarPalettes[hashText(`${name}:${normalizedSrc}`) % avatarPalettes.length];
    return {
      background: palette.background,
      color: palette.color,
    };
  }, [name, normalizedSrc]);

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
    <div
      className={`${className} avatar-fallback avatar-generated`}
      aria-label={name}
      style={fallbackStyle}
      title={normalizedSrc ? "Provider 头像不可用，已使用生成头像" : undefined}
    >
      <span>{initial}</span>
    </div>
  );
}
