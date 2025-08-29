import os
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
from langchain_mistralai import ChatMistralAI
from langchain_google_genai import ChatGoogleGenerativeAI

# For now use the env var to set the LLM provider later there will be a config
def get_llm():
    """ Return LLM client based on provider env var. \n
        Defaults to openai 
    """
    provider = os.getenv("LLM_PROVIDER", "openai").lower()

    if provider == "openai":
        return ChatOpenAI(
            model = "gpt-4",
            temperature = 0,
            api_key = os.getenv("OPENAI_API_KEY"),
        )
    elif provider == "anthropic":
        return ChatAnthropic(
            model = "claude-sonnet-4-20250514",
            temperature = 0,
            api_key = os.getenv("ANTHROPIC_API_KEY"),
        )
    elif provider == "mistral":
        return ChatMistralAI(
            model = "mistral-medium-2508",
            temperature = 0,
            api_key = os.getenv("MISTRAL_API_KEY"),
        )
    elif provider == "google":
        return ChatGoogleGenerativeAI(
            model = "gemini-2.5-flash",
            temperature = 0,
            api_key = os.getenv("GOOGLE_API_KEY"),
        )
