type LinkCard = {
  href: string;
  label: string;
  description: string;
  icon: React.ReactNode;
};

const LINKS: LinkCard[] = [
  {
    href: "https://github.com/mauritsarissen/OpenServiceBus",
    label: "GitHub",
    description: "Source, issues & releases",
    icon: (
      <svg viewBox="0 0 24 24" className="h-6 w-6" fill="currentColor">
        <path d="M12 .5C5.65.5.5 5.65.5 12c0 5.08 3.29 9.39 7.86 10.91.58.1.79-.25.79-.56v-2c-3.2.7-3.87-1.37-3.87-1.37-.52-1.33-1.27-1.69-1.27-1.69-1.04-.71.08-.69.08-.69 1.15.08 1.76 1.18 1.76 1.18 1.03 1.76 2.69 1.25 3.35.96.1-.75.4-1.25.73-1.54-2.55-.29-5.24-1.28-5.24-5.69 0-1.26.45-2.29 1.18-3.1-.12-.29-.51-1.46.11-3.05 0 0 .96-.31 3.15 1.18a10.94 10.94 0 0 1 5.74 0c2.19-1.49 3.15-1.18 3.15-1.18.62 1.59.23 2.76.11 3.05.74.81 1.18 1.84 1.18 3.1 0 4.42-2.7 5.4-5.26 5.68.41.36.78 1.06.78 2.14v3.17c0 .31.21.67.8.56C20.21 21.39 23.5 17.08 23.5 12 23.5 5.65 18.35.5 12 .5z" />
      </svg>
    ),
  },
  {
    href: "https://www.nuget.org/packages?q=OpenServiceBus",
    label: "NuGet",
    description: "Embed in your .NET tests",
    icon: (
      <svg viewBox="0 0 24 24" className="h-6 w-6" fill="none" stroke="currentColor" strokeWidth="1.8">
        <path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z" />
        <path d="m3.27 6.96 8.73 5.05 8.73-5.05M12 22.08V12" />
      </svg>
    ),
  },
  {
    href: "https://hub.docker.com/r/mauritsarissen/openservicebus",
    label: "Docker Hub",
    description: "Multi-arch image, ready to run",
    icon: (
      <svg viewBox="0 0 24 24" className="h-6 w-6" fill="currentColor">
        <path d="M13.983 11.078h2.119a.186.186 0 0 0 .185-.186V9.006a.186.186 0 0 0-.185-.186h-2.119a.185.185 0 0 0-.185.185v1.888c0 .102.083.185.185.185m-2.954-5.43h2.119a.186.186 0 0 0 .185-.186V3.574a.186.186 0 0 0-.185-.185h-2.119a.185.185 0 0 0-.184.185v1.888c0 .102.083.185.184.186m0 2.716h2.119a.187.187 0 0 0 .185-.186V6.29a.186.186 0 0 0-.185-.185h-2.119a.185.185 0 0 0-.184.185v1.887c0 .102.083.185.184.186m-2.93 0h2.12a.186.186 0 0 0 .184-.186V6.29a.185.185 0 0 0-.185-.185H8.1a.185.185 0 0 0-.185.185v1.887c0 .102.083.185.185.186m-2.964 0h2.119a.186.186 0 0 0 .185-.186V6.29a.185.185 0 0 0-.185-.185H5.136a.186.186 0 0 0-.186.185v1.887c0 .102.084.185.186.186m5.893 2.715h2.12a.186.186 0 0 0 .184-.186V9.006a.185.185 0 0 0-.184-.186h-2.12a.185.185 0 0 0-.184.185v1.888c0 .102.082.185.185.185m-2.93 0h2.12a.185.185 0 0 0 .184-.186V9.006a.185.185 0 0 0-.184-.186h-2.12a.185.185 0 0 0-.185.185v1.888c0 .102.084.185.185.185m-2.964 0h2.119a.185.185 0 0 0 .185-.186V9.006a.185.185 0 0 0-.184-.186h-2.12a.186.186 0 0 0-.185.185v1.888c0 .102.084.185.185.185m-2.92 0h2.12a.185.185 0 0 0 .184-.186V9.006a.185.185 0 0 0-.184-.186h-2.12a.185.185 0 0 0-.185.185v1.888c0 .102.083.185.185.185M23.763 9.89c-.065-.051-.672-.51-1.954-.51-.338.001-.676.03-1.01.087-.248-1.7-1.653-2.53-1.716-2.566l-.344-.199-.226.327c-.284.438-.49.922-.612 1.43-.23.97-.09 1.882.403 2.661-.595.332-1.55.413-1.744.42H.751a.751.751 0 0 0-.75.748 11.376 11.376 0 0 0 .692 4.062c.545 1.428 1.355 2.48 2.41 3.124 1.18.723 3.1 1.137 5.275 1.137.983.003 1.963-.086 2.93-.266a12.248 12.248 0 0 0 3.823-1.389c.98-.567 1.86-1.288 2.61-2.136 1.252-1.418 1.998-2.997 2.553-4.4h.221c1.372 0 2.215-.549 2.68-1.009.309-.293.55-.65.707-1.046l.098-.288Z" />
      </svg>
    ),
  },
];

export default function Links() {
  return (
    <section className="pb-20 sm:pb-24">
      <div className="grid gap-4 sm:grid-cols-3">
        {LINKS.map((link) => (
          <a
            key={link.label}
            href={link.href}
            target="_blank"
            rel="noreferrer"
            className="group flex items-center gap-4 rounded-xl border border-neutral-800 bg-neutral-900/40 px-5 py-4 transition hover:border-violet-500/40 hover:bg-neutral-900"
          >
            <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-neutral-800/70 text-neutral-300 transition group-hover:text-white">
              {link.icon}
            </div>
            <div>
              <div className="font-semibold text-neutral-100">{link.label}</div>
              <div className="text-xs text-neutral-400">{link.description}</div>
            </div>
            <svg
              viewBox="0 0 24 24"
              className="ml-auto h-4 w-4 text-neutral-600 transition group-hover:translate-x-0.5 group-hover:text-neutral-300"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
            >
              <path d="M7 17 17 7M7 7h10v10" />
            </svg>
          </a>
        ))}
      </div>
    </section>
  );
}
