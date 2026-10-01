import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { createBrowserRouter, Link, RouterProvider } from "react-router-dom";

import { Layout } from "./components/Layout";
import { OverviewPage } from "./pages/Overview";
import { RunDetailPage } from "./pages/RunDetail";
import { RunsPage } from "./pages/Runs";
import { SubmitPage } from "./pages/Submit";
import "./styles.css";

const router = createBrowserRouter([
  {
    element: <Layout />,
    children: [
      { path: "/", element: <OverviewPage /> },
      { path: "/runs", element: <RunsPage /> },
      { path: "/runs/:runId", element: <RunDetailPage /> },
      { path: "/submit", element: <SubmitPage /> },
      {
        path: "*",
        element: (
          <div className="page">
            <h1>Not found</h1>
            <Link to="/">Back to overview</Link>
          </div>
        ),
      },
    ],
  },
]);

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <RouterProvider router={router} />
  </StrictMode>,
);
