import re

blacklist = {
    "fuck", "bitch", "asshole", "idiot", "moron", "stupid", "dumb", "loser",
    "jerk", "clown", "trash", "pathetic", "worthless", "useless", "disgrace",
    "failure", "imbecile", "ignorant", "braindead", "clueless", "dunce", "nitwit"
}

def filter_insult(message):
    # Dividir por saltos de línea o por frases terminadas en ., ! o ?
    raw_sentences = re.split(r'(?:[.!?])?\s*\n+|\s*(?<=[.!?])\s+', message.strip())

    formatted_sentences = []
    for sentence in raw_sentences:
        if not sentence.strip():
            continue  # Saltar frases vacías
        words = sentence.split()
        filtered_words = [
            "CENSORED" if word.lower().strip(".,!?") in blacklist else word
            for word in words
        ]
        censored_sentence = " ".join(filtered_words)
        formatted_sentences.append(censored_sentence)

    # Unir cada frase censurada con salto de línea
    censored_message = "\n".join(formatted_sentences)
    return censored_message
