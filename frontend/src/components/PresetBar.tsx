import type { Preset } from "../lib/presets";
import { INVALID_PRESETS, VALID_PRESETS } from "../lib/presets";
import type { Draft } from "../lib/validate";
import "./PresetBar.css";

export function PresetBar({ onPick }: { onPick: (draft: Draft, preset: Preset) => void }) {
  const button = (preset: Preset) => (
    <button
      key={preset.label}
      type="button"
      className={`preset preset-${preset.expects}`}
      title={preset.description}
      onClick={() => onPick(preset.build(), preset)}
    >
      <span>{preset.label}</span>
      <code>{preset.expects}</code>
    </button>
  );

  return (
    <div className="presets">
      <div className="preset-group">
        <span className="preset-title">Realistic events</span>
        <div className="preset-list">{VALID_PRESETS.map(button)}</div>
      </div>
      <div className="preset-group">
        <span className="preset-title">Break one rule</span>
        <div className="preset-list">{INVALID_PRESETS.map(button)}</div>
      </div>
    </div>
  );
}
