## 🏀 NBA Analytics App
A full-stack application that allows users to ask natural language questions about NBA statistics and get accurate answers from real NBA data. It features a Python FastAPI backend, a React frontend, and integrates with various LLM providers (OpenAI, Google PaLM, Anthropic, Mistral, Ollama) for natural language processing.

## Quick start (Docker Compose)
1. Copy and fill environment variables:
   - Duplicate `.env.example` to `.env` and populate secrets.
2. Build and start services (runs backend on 8000 and frontend on 3000):
   - From project root:
     - `docker compose up -d`
3. View services:
   - Backend: http://localhost:8000
   - Frontend: http://localhost:3000
4. Stop and remove:
   - `docker compose down`

## Project structure
- backend/ — Python FastAPI backend, tests, data and storage adapters.
- frontend/ — Vite + React frontend.
- infra/ — Local LLM/infra utilities are available under this folder for experiments (Ollama, Terraform configs, etc).