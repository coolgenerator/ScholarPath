import { createRoot } from "react-dom/client";
import { BrowserRouter, Navigate, Route, Routes } from "react-router";
import App from "./app/App.tsx";
import { LandingPage } from "./app/pages/LandingPage";
import { LoginPage } from "./app/pages/LoginPage";
import { RegisterPage } from "./app/pages/RegisterPage";
import { ProfilePage } from "./app/pages/ProfilePage";
import { AppProvider } from "./context/AppContext";
import "./styles/index.css";

createRoot(document.getElementById("root")!).render(
  <BrowserRouter>
    <Routes>
      <Route path="/" element={<LandingPage />} />
      <Route path="/login" element={<LoginPage />} />
      <Route path="/register" element={<RegisterPage />} />
      <Route path="/profile" element={<ProfilePage />} />
      <Route
        path="/s/*"
        element={(
          <AppProvider>
            <App />
          </AppProvider>
        )}
      />
      <Route path="*" element={<Navigate replace to="/" />} />
    </Routes>
  </BrowserRouter>
);
