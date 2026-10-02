import numpy as np

from exploration.learning.production_tokenizer import (
    OurSearchProductionTokenizer,
)
from .conversation_memory import ConversationMemory


class PhoneBrain109MGenerator:
    VERSION = "phone-brain-109m-generator.v2"

    def __init__(self, model, temperature=0.8):
        self.model = model
        self.temperature = temperature
        self.tokenizer = OurSearchProductionTokenizer(
            vocabulary_size=4096
        )
        self.conversation_memory = ConversationMemory()

    def conversation_response(self, user_text):
        return self.conversation_memory.lookup(user_text)

    def next_token(self, tokens):
        logits = self.model.forward(
            np.asarray(tokens, dtype=np.int64)
        )

        last = logits[0, -1]

        if hasattr(last, "detach"):
            last = last.detach().cpu().numpy()

        vocabulary_size = min(
            self.model.storage.config.vocabulary_size,
            int(self.tokenizer.vocabulary_size),
            len(last),
        )

        last = last[:vocabulary_size]
        last = last / max(self.temperature, 1e-6)
        last = last - np.max(last)

        probabilities = np.exp(last)
        probabilities /= probabilities.sum() + 1e-12

        return int(
            np.random.choice(
                vocabulary_size,
                p=probabilities,
            )
        )

    def generate(self, prompt_tokens, max_new_tokens=32):
        tokens = list(prompt_tokens)

        eos_id = self.tokenizer.token_to_id.get(
            "<eos>"
        )

        for _ in range(max_new_tokens):
            token = self.next_token(tokens)
            tokens.append(token)

            if eos_id is not None and token == eos_id:
                break

        return tokens

    def respond(self, user_text, max_new_tokens=32):
        memory_response = self.conversation_response(
            user_text
        )

        if memory_response is not None:
            return memory_response

        prompt = (
            "<bos>User: "
            + str(user_text).strip()
            + "\nAssistant:"
        )

        prompt_tokens = self.encode(prompt)

        generated_tokens = self.generate(
            prompt_tokens,
            max_new_tokens=max_new_tokens,
        )

        return self.decode(generated_tokens)

    def encode(self, text):
        return self.tokenizer.encode(text)

    def decode(self, tokens):
        return self.tokenizer.decode(tokens)
