import Converter from "@/components/Converter";
import Hero from "@/components/Hero";

export default function Home() {
  return (
    <main className="grain min-h-screen px-6 py-10 md:px-12">
      <div className="mx-auto flex w-full max-w-6xl flex-col gap-10">
        <Hero />
        <Converter />
      </div>
    </main>
  );
}
