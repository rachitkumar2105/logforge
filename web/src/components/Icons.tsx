// Inline SVG icons on a 24 x 24 grid, 1.5 px stroke, round caps, currentColor.
import type { SVGProps } from "react";

type Props = SVGProps<SVGSVGElement> & { title?: string };

function Icon({ title, children, ...rest }: Props & { children: React.ReactNode }) {
  return (
    <svg
      width="20"
      height="20"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth={1.5}
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden={title ? undefined : true}
      role={title ? "img" : undefined}
      {...rest}
    >
      {title ? <title>{title}</title> : null}
      {children}
    </svg>
  );
}

export const BrokerIcon = (p: Props) => (
  <Icon {...p}>
    <rect x="4" y="4" width="16" height="6" rx="1" />
    <rect x="4" y="14" width="16" height="6" rx="1" />
    <path d="M8 7h.01M8 17h.01" />
  </Icon>
);

export const PlayIcon = (p: Props) => (
  <Icon {...p}>
    <path d="M8 5v14l11-7z" />
  </Icon>
);

export const PauseIcon = (p: Props) => (
  <Icon {...p}>
    <path d="M9 5v14M15 5v14" />
  </Icon>
);

export const StepBackIcon = (p: Props) => (
  <Icon {...p}>
    <path d="M18 6l-8 6 8 6V6zM6 6v12" />
  </Icon>
);

export const StepForwardIcon = (p: Props) => (
  <Icon {...p}>
    <path d="M6 6l8 6-8 6V6zM18 6v12" />
  </Icon>
);

export const PulseIcon = (p: Props) => (
  <Icon {...p}>
    <path d="M3 12h4l2-5 4 10 2-5h6" />
  </Icon>
);

export const WarningIcon = (p: Props) => (
  <Icon {...p}>
    <path d="M12 4l9 16H3z" />
    <path d="M12 10v4M12 17h.01" />
  </Icon>
);

export const GithubIcon = (p: Props) => (
  <Icon {...p}>
    <path d="M9 19c-4 1.3-4-2-6-2.5M15 21v-3.5a3 3 0 0 0-.9-2.4c3-.3 6-1.4 6-6.6a5.2 5.2 0 0 0-1.4-3.6 4.8 4.8 0 0 0-.1-3.6s-1.1-.3-3.7 1.4a12.7 12.7 0 0 0-6.6 0C5.7 1.1 4.6 1.4 4.6 1.4a4.8 4.8 0 0 0-.1 3.6A5.2 5.2 0 0 0 3 8.6c0 5.2 3 6.3 6 6.6a3 3 0 0 0-.9 2.3V21" />
  </Icon>
);

export const ExternalIcon = (p: Props) => (
  <Icon {...p}>
    <path d="M14 4h6v6M20 4l-9 9M18 14v5a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1V7a1 1 0 0 1 1-1h5" />
  </Icon>
);
