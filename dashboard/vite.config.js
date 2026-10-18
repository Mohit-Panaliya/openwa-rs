import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  build: {
    outDir: "dist",
    emptyOutDir: true,
    rollupOptions: {
      output: {
        entryFileNames: "app.js",
        chunkFileNames: "app.js",
        assetFileNames: (asset) => {
          if (asset.name && asset.name.endsWith(".css")) return "style.css";
          return "[name][extname]";
        },
      },
    },
  },
});