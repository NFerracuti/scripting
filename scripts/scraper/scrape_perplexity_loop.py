import os
import requests
import time
import json

from sysconfigs.client_creds import get_perplexity_credentials

api_key = get_perplexity_credentials()


def search_perplexity_loop(name, url):

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    prompt = f"For {name}, a gluten-free business whose website is {url}, can you find their Instagram?"

    payload = {
        "model": "llama-3.1-sonar-small-128k-online",
        "messages": [
            {
                "role": "system",
                "content": "You are a helpful assistant that finds Instagram URLs for businesses. Only return the Instagram URL or (none) if not found."
            },
            {
                "role": "user",
                "content": prompt
            }
        ]
    }

    try:
        response = requests.post(
            "https://api.perplexity.ai/chat/completions",
            json=payload,
            headers=headers
        )

        if response.status_code == 200:
            return response.json()['choices'][0]['message']['content'].strip()
        else:
            print(f"API error for {name}: {response.status_code}")
            if response.status_code == 401:
                print("Authentication failed. Please check your API key.")
            return "(none)"

    except Exception as e:
        print(f"Error processing {name}: {e}")
        return "(none)"


def process_vendors():
    vendors = [
        # ["Böld Quality Lemonaide", "https://www.boldqualitylemonaide.com"],
        # ["The Bread Essentials", "https://www.thebreadessentials.com"]
        ["Celiapp", "https://www.theceliapp.com"]

    ]

    results = []

    for name, url in vendors:
        print(f"\nProcessing: {name}")
        instagram = search_perplexity_loop(name, url)

        result = {
            "name": name,
            "url": url,
            "instagram": instagram
        }

        results.append(result)

        # Print current result
        print(f"Name: {name}")
        print(f"URL: {url}")
        print(f"Instagram: {instagram}")
        print("-" * 50)

        # Add delay between API calls
        time.sleep(2)

    # Save results to JSON
    with open('vendors_instagram.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2)

    return results


if __name__ == "__main__":
    results = process_vendors()
