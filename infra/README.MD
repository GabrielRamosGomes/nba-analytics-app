### Setup Application (Python Backend + React Frontend)
  - Inside `infra` folder, run the following commands to set up the backend and frontend
    ```sh
      cd infra
      docker-compose up -d
    ```
  - Access the React frontend at http://localhost:3000
  - Access the FastAPI backend at http://localhost:8000  

### Ollama (local models & web UI)
  - Incase you want to run the local models and web UI, follow these steps
  - How to run
    ```sh
      cd infra/ollama
      docker-compose up -d
      # After ther container are up, install the models you want to use
      docker exec -it ollama ollama install <model-name>
    ```
  - Access the web UI at http://localhost:11434
  - Make sure the environment variable `LLM_PROVIDER` is set to `ollama` in the `.env` file

### Terraform (cloud / infra provisioning)
 - Pre-requisites:
   - Install Terraform: https://learn.hashicorp.com/tutorials/terraform/install-cli
   - AWS CLI installed and configured: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html
   - AWS credentials configured (e.g., via `aws configure` or environment variables)
 - Run AWS setup
    ```sh
      cd infra/terraform
      terraform init
      terraform plan -out=tfplan
      terraform apply tfplan
    ```