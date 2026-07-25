interface BrandLogoProps {
  size?: "small" | "large";
}

export function BrandLogo({ size = "small" }: BrandLogoProps) {
  return (
    <span className={`brand-logo brand-logo-${size}`} aria-hidden="true">
      <svg viewBox="0 0 48 48" role="img">
        <circle cx="20" cy="20" r="11" fill="none" stroke="currentColor" strokeWidth="5" />
        <path
          d="M28.5 28.5 39 39"
          fill="none"
          stroke="currentColor"
          strokeWidth="5"
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </svg>
    </span>
  );
}
