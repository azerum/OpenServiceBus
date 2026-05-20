import Hero from "./components/Hero";
import Quickstart from "./components/Quickstart";
import Links from "./components/Links";
import Comparison from "./components/Comparison";
import Footer from "./components/Footer";

export default function App() {
  return (
    <div className="min-h-screen bg-neutral-950 text-neutral-100">
      <Hero />
      <main className="mx-auto max-w-5xl px-6">
        <Quickstart />
        <Links />
        <Comparison />
      </main>
      <Footer />
    </div>
  );
}
