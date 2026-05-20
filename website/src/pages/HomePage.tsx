import Nav from "../components/Nav";
import Hero from "../components/Hero";
import Quickstart from "../components/Quickstart";
import Links from "../components/Links";
import Comparison from "../components/Comparison";
import FAQ from "../components/FAQ";
import Footer from "../components/Footer";

export default function HomePage() {
  return (
    <>
      <Nav />
      <Hero />
      <main className="mx-auto max-w-5xl w-full min-w-0 px-4 sm:px-6">
        <Quickstart />
        <Links />
        <Comparison />
        <FAQ />
      </main>
      <Footer />
    </>
  );
}
