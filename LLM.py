import streamlit as st
from openai import OpenAI
from io import BytesIO

client = OpenAI(api_key=st.secrets["openai"]["openai_api_key"])
system_prompt1 = "Olet uutisten analysaattori"
task_description = "Seraavassa saat uutisotsikon. Päättele onko kysessä nk.klikkiotsikko joka jättää jotain olennaista kertomatta ja houkuttelee lukijaa klikkaamaan uutisen auki. Vastaa 1 jos kysessä klikkiotsikko. VAstaa 2 jos kyseessä ei ole klikkiotsikko: \n {}"
    

def get_LLM_response(user_text, task_description, system_prompt):
    try:
        formatted_task_description = task_description.format(user_text)

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": formatted_task_description}
        ]

        chat_completion = client.chat.completions.create(
            model="gpt-4-0125-preview",  
            messages=messages,
        )

        if chat_completion.choices:
            return chat_completion.choices[0].message.content
    except Exception as e:
        st.error(f"An error occurred: {e}")
        return "Error"


def process_headline(headline):
    # Call the get_LLM_response function with the headline
    response = get_LLM_response(headline, task_description, system_prompt1)
    return response