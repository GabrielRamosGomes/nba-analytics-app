import os
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
from langchain_mistralai import ChatMistralAI
from langchain_google_genai import ChatGoogleGenerativeAI
from ...core.settings import settings

# For now use the env var to set the LLM provider later there will be a config
def get_llm():
    """ Return LLM client based on provider env var. \n
        Defaults to openai 
    """
    provider = settings.get_env_var("LLM_PROVIDER", "openai").lower()  
    if provider == "openai":
        return ChatOpenAI(
            model = "gpt-4o-mini",
            temperature = 0,
            api_key = settings.get_env_var("OPENAPI_API_KEY"),
        )
    elif provider == "anthropic":
        return ChatAnthropic(
            model = "claude-3-5-sonnet-20241022",
            temperature = 0,
            api_key = settings.get_env_var("ANTHROPIC_API_KEY"),
        )
    elif provider == "mistral":
        return ChatMistralAI(
            model = "mistral-small-latest",
            temperature = 0,
            api_key = settings.get_env_var("MISTRAL_API_KEY"),
        )
    elif provider == "google":
        return ChatGoogleGenerativeAI(
            model = "gemini-2.5-flash",
            temperature = 0,
            api_key = settings.get_env_var("GOOGLE_API_KEY"),
        )
