interface SearchComposerProps {
  value: string;
  isSubmitting: boolean;
  compact?: boolean;
  showWelcome?: boolean;
  promptExamples: string[];
  onChange: (value: string) => void;
  onSubmit: (value: string) => void;
}

export function SearchComposer({
  value,
  isSubmitting,
  compact = false,
  showWelcome = true,
  promptExamples,
  onChange,
  onSubmit,
}: SearchComposerProps) {
  const placeholder = "告诉我你想找什么样的人...";

  return (
    <section className={`composer-shell${compact ? " compact" : ""}`}>
      <div className="composer-card">
        {!compact && showWelcome ? (
          <div className="welcome-block">
            <span className="welcome-kicker">Welcome Back</span>
          </div>
        ) : null}
        <div className="composer-input-wrap">
          <textarea
            className="composer-input"
            value={value}
            placeholder={placeholder}
            onChange={(event) => onChange(event.target.value)}
            rows={compact ? 3 : 4}
          />
          <button
            type="button"
            className="primary-button composer-submit"
            disabled={isSubmitting || !value.trim()}
            onClick={() => onSubmit(value)}
          >
            {isSubmitting ? "生成中..." : "开始搜索"}
          </button>
        </div>

        <div className="prompt-grid">
          {promptExamples.map((prompt) => (
            <button key={prompt} type="button" className="prompt-card" onClick={() => onSubmit(prompt)}>
              {prompt}
            </button>
          ))}
        </div>
      </div>
    </section>
  );
}
