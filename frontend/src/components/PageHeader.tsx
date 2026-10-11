import type { ReactNode } from "react";

interface Props {
  title: string;
  // The endpoint or process this screen reads, shown so a viewer can map
  // what they see back to the backend that produced it.
  stage: string;
  children: ReactNode;
}

export function PageHeader({ title, stage, children }: Props) {
  return (
    <header className="page-header">
      <span className="stage-tag">{stage}</span>
      <h1>{title}</h1>
      <p>{children}</p>
    </header>
  );
}
