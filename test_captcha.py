# https://aistudio.google.com/u/1/usage?project=gen-lang-client-0290532217&timeRange=last-1-day&tab=rate-limit
import os
import asyncio
from dotenv import load_dotenv
from google import genai
from google.genai import types

# 1. Load Environment Variables
load_dotenv()
API_KEY = os.getenv("GEMINI_API_KEY")

if not API_KEY:
    print("❌ Error: GEMINI_API_KEY not found in .env file.")
    exit(1)

# ================= CONFIGURATION =================
# Change the model name here to test different versions
# Options from the link above:
# "gemini-2.5-flash", "gemini-2.5-flash-lite", "gemini-robotics-er-1.5-preview", 
MODEL_NAME = "gemini-robotics-er-1.5-preview"

IMAGE_PATH = "Img/_captcha.jpg"

# In the real bot, these options come from the telegram buttons.
# For this test, we simulate a list of options. 
# Update this list to match what is usually in your captcha (or leave it generic).
SIMULATED_BUTTON_OPTIONS = ["🦉", "⚔", "💙", "🖐🏿", "🟪", "🍼"]
# =================================================

async def test_solve_captcha():
    print(f"🔄 Initializing Gemini Client with model: {MODEL_NAME}...")
    
    try:
        client = genai.Client(api_key=API_KEY)
    except Exception as e:
        print(f"❌ Failed to create client: {e}")
        return

    # 2. Load Image
    if not os.path.exists(IMAGE_PATH):
        print(f"❌ Error: Image not found at {IMAGE_PATH}")
        print("   Please create the folder 'Img' and put a file named 'captcha.jpg' inside.")
        return

    print(f"📂 Reading image from: {IMAGE_PATH}")
    try:
        with open(IMAGE_PATH, "rb") as f:
            image_data = f.read()
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return

    # 3. Prepare Prompt (Exact logic from main.py)
    # We ask it to select from the simulated list
    prompt = (
        f"Look at the object in this image. "
        f"Select the most appropriate emoji from this list: {SIMULATED_BUTTON_OPTIONS}. "
        f"Return only the emoji character itself."
    )

    print(f"📤 Sending request to Google AI...")
    
    try:
        # 4. API Call
        # Note: Using asyncio.to_thread just like in your main.py to simulate async behavior
        response = await asyncio.to_thread(
            client.models.generate_content,
            model=MODEL_NAME,
            contents=[
                types.Part.from_bytes(data=image_data, mime_type="image/jpeg"),
                prompt
            ]
        )

        # 5. Output Result
        if response.text:
            result = response.text.strip()
            print("\n" + "="*30)
            print(f"✅ SUCCESS!")
            print(f"🤖 Model: {MODEL_NAME}")
            print(f"🎯 Answer: {result}")
            print("="*30 + "\n")
        else:
            print("⚠️ Response received but text was empty.")

    except Exception as e:
        print(f"\n❌ API Error: {e}")
        if "404" in str(e):
            print("   (Hint: The model name might be wrong or not available in your region)")
        if "429" in str(e):
            print("   (Hint: Quota exceeded or rate limit reached)")

def list_available_models():
    print(f"🔄 Authenticating with Google GenAI...")
    try:
        client = genai.Client(api_key=API_KEY)
        
        print("\n📋 Fetching list of available models...\n")
        
        # Call the list method
        # Note: The SDK typically returns an iterable or a pager
        pager = client.models.list()
        
        found_any = False
        print(f"{'Model Name (ID)':<40} | {'Display Name'}")
        print("-" * 70)
        
        for model in pager:
            found_any = True
            # Some SDK versions require fully qualified names (models/gemini-1.5-flash)
            # others accept just the short name. The 'name' attribute usually has the full ID.
            print(f"{model.name:<40} | {model.display_name}")
            
        print("-" * 70)
        
        if not found_any:
            print("⚠️ No models found. Check your API key permissions.")
            
    except Exception as e:
        print(f"❌ Error listing models: {e}")

if __name__ == "__main__":
    # list_available_models()
    asyncio.run(test_solve_captcha())