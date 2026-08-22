import type { Metadata } from 'next';
import './globals.css';
import '../public/app.css';

export const metadata: Metadata = {
  title: 'Paper Desk Cloud',
  description: 'Source-grounded price-action advisory and paper trading workspace.',
  openGraph: { title: 'Paper Desk Cloud', description: 'Private, source-grounded price-action advisory and paper trading workspace.' },
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return <html lang="zh-CN"><head><meta name="pa-page-token" content="cloud" /></head><body>{children}</body></html>;
}
