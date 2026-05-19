import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Harness Workbench",
  description: "Phase 4 frontend MVP for the harness platform"
};

export default function RootLayout({
  children
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
