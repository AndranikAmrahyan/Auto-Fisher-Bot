# https://aistudio.google.com/u/1/usage?project=gen-lang-client-0290532217&timeRange=last-1-day&tab=rate-limit
import os
import asyncio
import io
import re
from dotenv import load_dotenv
from google import genai
from google.genai import types
from PIL import Image

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
MODEL_NAME = "gemini-2.5-flash-lite"

IMAGE_PATH = "Img/sword3.jpg"

# In the real bot, these options come from the telegram buttons.
# For this test, we simulate a list of options. 
# Update this list to match what is usually in your captcha (or leave it generic).
SIMULATED_BUTTON_OPTIONS = ["⏰", "⚔", "💼", "💸", "🥵", "💍"]
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
    raw_img_bytes = None
    try:
        with open(IMAGE_PATH, "rb") as f:
            raw_img_bytes = f.read()
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        return

    # === ИНТЕГРАЦИЯ: ОБРАБОТКА ИЗОБРАЖЕНИЯ (CROP) ===
    # Используем PIL для обрезки правой части (текст "ПРОВЕРКА НА РОБОТА...")
    final_image_data = raw_img_bytes
    try:
        with Image.open(io.BytesIO(raw_img_bytes)) as img:
            width, height = img.size
            # Отрезаем ~35% справа, оставляем левые 65%
            crop_width = int(width * 0.65)
            
            # Обрезаем: (left, top, right, bottom)
            cropped_img = img.crop((0, 0, crop_width, height))
            
            # Сохраняем обработанное изображение в буфер
            output_buffer = io.BytesIO()
            # Конвертируем в RGB если нужно
            if img.mode in ("RGBA", "P"):
                cropped_img = cropped_img.convert("RGB")
                
            cropped_img.save(output_buffer, format="JPEG")
            final_image_data = output_buffer.getvalue()
            print("✂️ Image cropped successfully (removed right 35%)")
    except Exception as pil_err:
        print(f"⚠️ Warning: PIL processing failed, using original image: {pil_err}")
        final_image_data = raw_img_bytes

    # 3. Prepare Prompt (Updated from main.py)
    prompt = (
        f"This is a captcha check. The image contains one MAIN object which is significantly LARGER than the others. "
        f"There are also small decoy icons and chaotic lines - IGNORE them. "
        f"Look strictly for the single BIGGEST visual element in the image. "
        f"Compare this biggest object with the following emoji options: {', '.join(SIMULATED_BUTTON_OPTIONS)}. "
        f"Reply with ONLY the single emoji character from the list that matches the biggest object. "
        f"Do not write explanations."
    )

    print(f"📤 Sending request to Google AI...")
    
    try:
        # 4. API Call
        response = await asyncio.to_thread(
            client.models.generate_content,
            model=MODEL_NAME,
            contents=[
                types.Part.from_bytes(data=final_image_data, mime_type="image/jpeg"),
                prompt
            ]
        )

        # 5. Output Result with Smart Parsing
        if response.text:
            raw_answer = response.text.strip()
            print(f"\n📩 Raw API Response: '{raw_answer}'")
            
            # === ИНТЕГРАЦИЯ: УМНЫЙ ПАРСИНГ ОТВЕТА ===
            predicted_emoji = None
            
            # 1. Точное совпадение
            if raw_answer in SIMULATED_BUTTON_OPTIONS:
                predicted_emoji = raw_answer
            
            # 2. Поиск эмодзи внутри текста (если нет точного совпадения)
            if not predicted_emoji:
                # Собираем все варианты из SIMULATED_BUTTON_OPTIONS, которые ИИ упомянул в своем ответе
                found_options = [opt for opt in SIMULATED_BUTTON_OPTIONS if opt in raw_answer]
                
                if len(found_options) == 1:
                    # Если найден ровно один вариант — это наш выбор
                    predicted_emoji = found_options[0]
                elif len(found_options) > 1:
                    # Если ИИ упомянул больше одного варианта из доступных кнопок
                    print("\n" + "="*30)
                    print("❌ FAILURE (AMBIGUOUS RESPONSE)")
                    print(f"❓ Found multiple options: {found_options}")
                    print(f"📄 Full response: {raw_answer}")
                    print("="*30 + "\n")
                    return # Stop here as per logic
            
            if predicted_emoji:
                print("\n" + "="*30)
                print(f"✅ SUCCESS!")
                print(f"🤖 Model: {MODEL_NAME}")
                print(f"🎯 Decoded Answer: {predicted_emoji}")
                print("="*30 + "\n")
            else:
                print("\n" + "="*30)
                print("❌ FAILURE (NO MATCH)")
                print("⚠️ No valid emoji from the list found in response.")
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
        
        pager = client.models.list()
        
        found_any = False
        print(f"{'Model Name (ID)':<40} | {'Display Name'}")
        print("-" * 70)
        
        for model in pager:
            found_any = True
            print(f"{model.name:<40} | {model.display_name}")
            
        print("-" * 70)
        
        if not found_any:
            print("⚠️ No models found. Check your API key permissions.")
            
    except Exception as e:
        print(f"❌ Error listing models: {e}")

if __name__ == "__main__":
    # list_available_models()
    asyncio.run(test_solve_captcha())
