
import google.generativeai as genai
import anthropic
import time
import os
import requests
from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Generator
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass
class ModelConfig:
    name: str
    display_name: str
    provider: str
    description: str = ""
    supports_streaming: bool = False
    max_tokens: int = 4096
    default_temperature: float = 0.7


class LLMProvider(ABC):
    """Abstract base class for LLM providers."""
    
    @abstractmethod
    def generate(self, prompt: str, history: Optional[List[Dict]] = None, 
                 temperature: float = 0.7, max_tokens: int = 4096) -> Optional[str]:
        """Generate a response from the model."""
        pass
    
    @abstractmethod
    def is_available(self) -> bool:
        """Check if the provider is properly configured and available."""
        pass
    
    @property
    @abstractmethod
    def model_config(self) -> ModelConfig:
        """Return the model configuration."""
        pass


class GeminiProvider(LLMProvider):
    
    def __init__(self, model_name: str = "gemini-2.5-flash"):
        self.api_key = os.getenv("api_key")
        self.model_name = model_name
        self._model = None
        
        # Rate limiting
        self.tokens_per_minute = 250000
        self.last_request_time = 0
        self.tokens_used_this_minute = 0
        
        if self.api_key:
            genai.configure(api_key=self.api_key)
            self._model = genai.GenerativeModel(model_name)
    
    @property
    def model_config(self) -> ModelConfig:
        return ModelConfig(
            name="gemini",
            display_name="Gemini 2.5 (Google Cloud)",
            provider="google",
            description="Google's Gemini 2.5 Flash model - fast and capable",
            supports_streaming=True,
            max_tokens=8192,
            default_temperature=0.7
        )
    
    def is_available(self) -> bool:
        return self.api_key is not None and self._model is not None
    
    def _estimate_tokens(self, text: str) -> int:
        return len(text) // 4
    
    def _wait_for_rate_limit(self, estimated_tokens: int) -> float:
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last > 60:
            self.tokens_used_this_minute = 0
            self.last_request_time = current_time
        
        if self.tokens_used_this_minute + estimated_tokens > self.tokens_per_minute:
            wait_time = 60 - time_since_last
            if wait_time > 0:
                return wait_time
        return 0
    
    def generate(self, prompt: str, history: Optional[List[Dict]] = None,
                 temperature: float = 0.7, max_tokens: int = 4096) -> Optional[str]:
        if not self.is_available():
            return None
        
        try:
            # Build full prompt with history
            if history:
                history_str = ""
                for msg in history[-6:]:
                    role = "User" if msg["role"] == "user" else "Assistant"
                    history_str += f"{role}: {msg['content']}\n"
                full_prompt = f"{history_str}User: {prompt}\n\nRespond helpfully to the user's message."
            else:
                full_prompt = f"{prompt}\n\nRespond helpfully to the user's message."
            
            # Rate limiting
            estimated_tokens = self._estimate_tokens(full_prompt)
            wait_time = self._wait_for_rate_limit(estimated_tokens)
            if wait_time > 0:
                time.sleep(wait_time)
            
            # Generate response
            response = self._model.generate_content(contents=full_prompt)
            
            # Update token usage
            current_time = time.time()
            if current_time - self.last_request_time > 60:
                self.tokens_used_this_minute = estimated_tokens
                self.last_request_time = current_time
            else:
                self.tokens_used_this_minute += estimated_tokens
            
            return response.text.strip()
        except Exception as e:
            print(f"Gemini API error: {str(e)}")
            return None


class ClaudeProvider(LLMProvider):
    
    def __init__(self, model_name: str = "claude-opus-4-5-20251101"):
        self.api_key = os.getenv("claude_api_key")
        self.model_name = model_name
        self._client = None
        
        if self.api_key:
            self._client = anthropic.Anthropic(api_key=self.api_key)
    
    @property
    def model_config(self) -> ModelConfig:
        return ModelConfig(
            name="claude",
            display_name="Claude Opus 4.5 (Anthropic)",
            provider="anthropic",
            description="Anthropic's most capable Claude model",
            supports_streaming=True,
            max_tokens=4096,
            default_temperature=0.7
        )
    
    def is_available(self) -> bool:
        return self._client is not None
    
    def generate(self, prompt: str, history: Optional[List[Dict]] = None,
                 temperature: float = 0.7, max_tokens: int = 4096) -> Optional[str]:
        if not self.is_available():
            return None
        
        try:
            messages = []
            if history:
                for msg in history[-10:]:
                    messages.append({
                        "role": msg["role"],
                        "content": msg["content"]
                    })
            
            messages.append({"role": "user", "content": prompt})
            
            response = self._client.messages.create(
                model=self.model_name,
                max_tokens=max_tokens,
                system="You are a helpful AI assistant. Answer questions directly and helpfully in the same language as the user. Be conversational and informative.",
                messages=messages
            )
            
            return response.content[0].text.strip()
        except Exception as e:
            print(f"Claude API error: {str(e)}")
            return None


class LocalLLMProvider(LLMProvider):
    
    def __init__(self, name: str, display_name: str, endpoint_env_var: str, description: str = ""):
        self._name = name
        self._display_name = display_name
        self._description = description
        self.endpoint = os.getenv(endpoint_env_var)
    
    @property
    def model_config(self) -> ModelConfig:
        return ModelConfig(
            name=self._name,
            display_name=self._display_name,
            provider="local",
            description=self._description,
            supports_streaming=False,
            max_tokens=512,
            default_temperature=0.3
        )
    
    def is_available(self) -> bool:
        return self.endpoint is not None
    
    def generate(self, prompt: str, history: Optional[List[Dict]] = None,
                 temperature: float = 0.3, max_tokens: int = 512) -> Optional[str]:
        if not self.is_available():
            return None
        
        try:
            # Build conversation context
            if history:
                history_str = ""
                for msg in history[-6:]:
                    role = "User" if msg["role"] == "user" else "Assistant"
                    history_str += f"{role}: {msg['content']}\n"
                user_prompt = f"{history_str}User: {prompt}"
            else:
                user_prompt = prompt
            
            # Format for local model
            formatted_prompt = f"""### System:
You are a helpful AI assistant. Answer questions directly and concisely in the same language as the user. Do not write code unless asked. Do not analyze the question - just answer it.

### User:
{user_prompt}

### Assistant:
"""
            
            headers = {
                "Content-Type": "application/json",
                "Authorization": "Bearer dummy-key"
            }
            
            payload = {
                "prompt": formatted_prompt,
                "max_tokens": max_tokens,
                "temperature": temperature,
                "top_p": 0.9,
                "stop": ["\n### User:", "\n### System:", "\nUser:", "###", "<|endoftext|>", "<|end|>", "\n\n\n"],
                "repetition_penalty": 1.15
            }
            
            response = requests.post(
                self.endpoint,
                headers=headers,
                json=payload,
                timeout=120
            )
            response.raise_for_status()
            
            result = response.json()
            
            if "choices" in result and len(result["choices"]) > 0:
                text = result["choices"][0].get("text", "")
                if not text and "message" in result["choices"][0]:
                    text = result["choices"][0]["message"].get("content", "")
                text = text.strip()
                if "### User:" in text:
                    text = text.split("### User:")[0].strip()
                if "### System:" in text:
                    text = text.split("### System:")[0].strip()
                return text
            return None
        except Exception as e:
            print(f"Local API error ({self.endpoint}): {str(e)}")
            return None


class LLMClient:
    
    def __init__(self):
        self._providers: Dict[str, LLMProvider] = {}
        self._register_default_providers()
    
    def _register_default_providers(self):
        """Register the default set of providers."""
        # Cloud providers
        self.register_provider(GeminiProvider())
        self.register_provider(ClaudeProvider())
        
        # Local providers
        self.register_provider(LocalLLMProvider(
            name="gemma",
            display_name="Gemma 3 (Paikallinen)",
            endpoint_env_var="GEMMA_API",
            description="Google's Gemma 3 running locally"
        ))
        self.register_provider(LocalLLMProvider(
            name="oss",
            display_name="OSS-120B (Paikallinen)",
            endpoint_env_var="OSS_API",
            description="Open source 120B parameter model running locally"
        ))
    
    def register_provider(self, provider: LLMProvider):
        """Register a new LLM provider."""
        self._providers[provider.model_config.name] = provider
    
    def get_provider(self, model_name: str) -> Optional[LLMProvider]:
        """Get a provider by model name."""
        return self._providers.get(model_name)
    
    def get_available_models(self) -> List[ModelConfig]:
        """Get list of available model configurations."""
        return [
            provider.model_config 
            for provider in self._providers.values() 
            if provider.is_available()
        ]
    
    def get_all_models(self) -> List[ModelConfig]:
        """Get list of all registered model configurations."""
        return [provider.model_config for provider in self._providers.values()]
    
    def get_model_choices(self) -> List[tuple]:
        """Get model choices formatted for Gradio Radio/Dropdown."""
        return [
            (config.display_name, config.name)
            for config in self.get_all_models()
        ]
    
    def chat(self, message: str, history: Optional[List[Dict]] = None,
             model: str = "gemini", temperature: float = 0.7,
             max_tokens: int = 4096) -> Optional[str]:
        """
        Send a message to a model and get a response.
        
        Args:
            message: The user's message
            history: Optional conversation history
            model: Model name to use
            temperature: Sampling temperature
            max_tokens: Maximum tokens in response
            
        Returns:
            The model's response or None if error
        """
        provider = self.get_provider(model)
        if not provider:
            return f"Model '{model}' not found."
        
        if not provider.is_available():
            return f"Model '{model}' is not available. Check API configuration."
        
        return provider.generate(message, history, temperature, max_tokens)


class DelphiPanel:
    """
    Delphi Panel: Multiple AI models discuss a topic together.
    Simulates an expert panel discussion with different AI perspectives.
    """
    
    def __init__(self, llm_client: LLMClient):
        self.llm_client = llm_client
        self.discussion_history: List[Dict] = []
    
    def get_available_panelists(self) -> List[str]:
        """Get list of available models for the panel."""
        return [config.name for config in self.llm_client.get_available_models()]
    
    def reset_discussion(self):
        """Reset the discussion history."""
        self.discussion_history = []
    
    def get_panelist_persona(self, model_name: str) -> str:
        """Get a persona description for each panelist."""
        personas = {
            "gemini": "Analyyttinen asiantuntija, joka keskittyy faktoihin ja loogiseen analyysiin.",
            "claude": "Filosofinen ajattelija, joka pohtii asiaa monesta näkökulmasta ja nostaa esiin eettisiä kysymyksiä.",
            "gemma": "Käytännönläheinen asiantuntija, joka tuo esiin konkreettisia esimerkkejä ja ratkaisuja.",
            "oss": "Kriittinen arvioija, joka kyseenalaistaa oletuksia ja etsii vaihtoehtoisia näkökulmia."
        }
        return personas.get(model_name, "Asiantuntija, joka tuo oman näkökulmansa keskusteluun.")
    
    def format_discussion_context(self, topic: str, initial_context: str = "",
                                   num_rounds: int = 3) -> str:
        """Format the discussion context for a panelist."""
        context = f"""Olet osa Delfoi-asiantuntijapaneelia, jossa useat tekoälyt keskustelevat annetusta aiheesta.

AIHE: {topic}

"""
        if initial_context:
            context += f"""TAUSTATIETOA KESKUSTELUUN:
{initial_context}

"""
        
        if self.discussion_history:
            context += "KESKUSTELUHISTORIA:\n"
            for entry in self.discussion_history:
                context += f"\n{entry['display_name']}:\n{entry['response']}\n"
        
        return context
    
    def run_round(self, topic: str, panelists: List[str], 
                  initial_context: str = "", temperature: float = 0.7,
                  max_tokens: int = 1024, round_num: int = 1,
                  total_rounds: int = 3,
                  custom_personas: Optional[Dict[str, str]] = None) -> Generator[Dict, None, None]:
        """
        Run one round of discussion with all panelists.
        
        Args:
            custom_personas: Optional dict mapping model_name to custom persona string
        
        Yields:
            Dict with 'model', 'display_name', and 'response' for each panelist
        """
        for model_name in panelists:
            provider = self.llm_client.get_provider(model_name)
            if not provider or not provider.is_available():
                continue
            
            config = provider.model_config
            # Use custom persona if provided, otherwise use default
            if custom_personas and model_name in custom_personas:
                persona = custom_personas[model_name]
            else:
                persona = self.get_panelist_persona(model_name)
            context = self.format_discussion_context(topic, initial_context)
            
            is_last_round = (round_num == total_rounds)
            
            if round_num == 1:
                prompt = f"""{context}
Sinun roolisi: {persona}

Tämä on keskustelun kierros {round_num}/{total_rounds}.

Anna oma asiantuntija-analyysisi aiheesta. Ole ytimekäs mutta kattava (max 200 sanaa).
Tuo esiin oma näkökulmasi ja mahdollisia näkökohtia, joita muut eivät ehkä huomioi.

Vastaa suoraan ilman johdantolauseita kuten "Asiantuntijana ajattelen..." tai vastaavia."""
            elif is_last_round:
                # Last round: seek consensus
                prompt = f"""{context}
Sinun roolisi: {persona}

Tämä on keskustelun VIIMEINEN kierros ({round_num}/{total_rounds}).

Pyri nyt rakentamaan yhteistä näkemystä ja konsensusta:
- Tunnista keskeiset yhteiset näkemykset, joista panelistit ovat samaa mieltä
- Ehdota kompromisseja erimielisyyksiin
- Muotoile konkreettisia johtopäätöksiä tai suosituksia
- Nosta esiin mahdolliset jäljelle jäävät avoimet kysymykset

Ole ytimekäs (max 200 sanaa). Vastaa suoraan ilman johdantolauseita."""
            else:
                prompt = f"""{context}
Sinun roolisi: {persona}

Tämä on keskustelun kierros {round_num}/{total_rounds}.

Lue muiden panelistien kommentit ja vastaa niihin:
- Nosta esiin asioita, joista olet samaa tai eri mieltä
- Täydennä tai haasta aiempia näkökulmia
- Tuo esiin uusia näkökulmia, joita ei ole vielä käsitelty

Ole ytimekäs (max 150 sanaa). Vastaa suoraan ilman johdantolauseita."""
            
            response = provider.generate(prompt, temperature=temperature, max_tokens=max_tokens)
            
            if response:
                entry = {
                    'model': model_name,
                    'display_name': config.display_name,
                    'response': response,
                    'round': round_num
                }
                self.discussion_history.append(entry)
                yield entry
    
    def generate_summary(self, topic: str, summarizer_model: str = "gemini") -> Optional[str]:
        """Generate a summary of the discussion."""
        if not self.discussion_history:
            return None
        
        provider = self.llm_client.get_provider(summarizer_model)
        if not provider or not provider.is_available():
            # Try to find any available provider
            for model_name in self.get_available_panelists():
                provider = self.llm_client.get_provider(model_name)
                if provider and provider.is_available():
                    break
            else:
                return None
        
        # Build discussion text
        discussion_text = ""
        for entry in self.discussion_history:
            discussion_text += f"\n{entry['display_name']} (kierros {entry['round']}):\n{entry['response']}\n"
        
        prompt = f"""Olet moderaattori Delfoi-paneelissa. Alla on asiantuntijoiden keskustelu aiheesta:

AIHE: {topic}

KESKUSTELU:
{discussion_text}

Tee yhteenveto keskustelusta:
1. Mitkä olivat keskeiset näkökulmat?
2. Mistä asiantuntijat olivat samaa mieltä?
3. Mistä he olivat eri mieltä?
4. Mitä johtopäätöksiä voidaan vetää?

Vastaa suomeksi, selkeästi ja jäsennellysti."""
        
        return provider.generate(prompt, temperature=0.5, max_tokens=2048)


class GeminiClient(LLMClient):
    """Backwards compatible alias for LLMClient."""
    pass
