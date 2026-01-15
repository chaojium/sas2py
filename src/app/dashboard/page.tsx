import DashboardClient from "@/components/DashboardClient";

export default function DashboardPage() {
  return (
    <main className="grain min-h-screen px-6 py-10 md:px-12">
      <div className="mx-auto w-full max-w-6xl">
        <DashboardClient />
      </div>
    </main>
  );
}
