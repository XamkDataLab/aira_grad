import gradio as gr
from .llm_client import LLMClient, DelphiPanel
from db import execute_query

# Initialize LLM client and Delphi panel
llm_client = LLMClient()
delphi_panel = DelphiPanel(llm_client)


def load_riskiarvio_documents():
    """Load riskiarvio documents from database for ids 12 and 13"""
    query = "SELECT id, text, pdf_file_year FROM riskiarviot WHERE id IN (12, 13) ORDER BY id"
    df, error = execute_query(query)
    if error:
        return None, None, error
    
    docs = {}
    for _, row in df.iterrows():
        docs[row['id']] = {
            'text': row['text'],
            'year': row['pdf_file_year']
        }
    return docs.get(12), docs.get(13), None


def build_kielimalliavustin_tab(demo):
    """Build the Kielimalliavustin tab with chat and document chat functionality"""
    
    gr.Markdown("## Kielimalliavustin - Keskustele tekoälyn kanssa")
    
    # General Chat Section
    gr.Markdown("### Yleinen keskustelu")
    
    # Model selector - dynamically get choices from LLMClient
    model_selector = gr.Radio(
        choices=llm_client.get_model_choices(),
        value="gemini",
        label="Valitse malli",
        interactive=True
    )
    
    # Chat interface
    chatbot = gr.Chatbot(
        label="Keskustelu",
        height=400,
        type="messages"
    )
    
    with gr.Row():
        msg_input = gr.Textbox(
            label="Kirjoita viestisi",
            placeholder="Kirjoita kysymyksesi tähän...",
            scale=9,
            show_label=False
        )
        send_btn = gr.Button("Lähetä", scale=1, variant="primary")
    
    clear_btn = gr.Button("Tyhjennä keskustelu", variant="secondary")
    
    def respond(message, chat_history, selected_model):
        if not message.strip():
            return "", chat_history
        
        # Convert chat history to the format expected by LLMClient
        history_for_llm = []
        for msg in chat_history:
            history_for_llm.append({
                "role": msg["role"],
                "content": msg["content"]
            })
        
        # Get response from selected model
        bot_response = llm_client.chat(message, history_for_llm, model=selected_model)
        
        if bot_response is None:
            bot_response = "Pahoittelut, kohtasin virheen vastausta luodessa. Yritä uudelleen."
        
        # Add messages to chat history
        chat_history.append({"role": "user", "content": message})
        chat_history.append({"role": "assistant", "content": bot_response})
        
        return "", chat_history
    
    def clear_chat():
        return []
    
    # Event handlers
    msg_input.submit(respond, [msg_input, chatbot, model_selector], [msg_input, chatbot])
    send_btn.click(respond, [msg_input, chatbot, model_selector], [msg_input, chatbot])
    clear_btn.click(clear_chat, None, chatbot)
    
    # Separator
    gr.Markdown("---")
    
    # Riskiarvio Document Chat Section
    gr.Markdown("### Kymenlaakson riskiarviot - dokumenttikeskustelu")
    gr.Markdown("Lataa Kymenlaakson riskiarvio-dokumentit (kahdelta eri vuodelta) ja keskustele niiden sisällöstä tekoälyn kanssa.")
    
    # State to hold the document context
    doc_context_state = gr.State(value=None)
    
    with gr.Row():
        load_docs_btn = gr.Button("Lataa riskiarvio-dokumentit", variant="primary")
        doc_status = gr.Textbox(label="Tila", interactive=False, scale=2)
    
    # Document chat (initially hidden)
    with gr.Column(visible=False) as doc_chat_section:
        doc_chatbot = gr.Chatbot(
            label="Dokumenttikeskustelu",
            height=400,
            type="messages"
        )
        
        with gr.Row():
            doc_msg_input = gr.Textbox(
                label="Kirjoita kysymyksesi dokumenteista",
                placeholder="Kysy jotain riskiarvio-dokumenteista...",
                scale=9,
                show_label=False
            )
            doc_send_btn = gr.Button("Lähetä", scale=1, variant="primary")
        
        doc_clear_btn = gr.Button("Tyhjennä keskustelu", variant="secondary")
    
    def load_documents():
        doc1, doc2, error = load_riskiarvio_documents()
        
        if error:
            return None, f"Virhe ladattaessa dokumentteja: {error}", gr.update(visible=False)
        
        if not doc1 or not doc2:
            return None, "Dokumentteja ei löytynyt tietokannasta", gr.update(visible=False)
        
        # Build context string
        context = f"""Sinulla on käytössäsi kaksi Kymenlaakson riskiarvio-dokumenttia eri vuosilta. 
Käyttäjä haluaa kysyä näistä dokumenteista ja vertailla niitä.

=== DOKUMENTTI 1: Kymenlaakson riskiarvio (vuosi {doc1['year']}) ===
{doc1['text']}

=== DOKUMENTTI 2: Kymenlaakson riskiarvio (vuosi {doc2['year']}) ===
{doc2['text']}

Vastaa käyttäjän kysymyksiin näiden dokumenttien pohjalta. Voit vertailla dokumentteja, nostaa esiin eroja ja yhtäläisyyksiä, ja analysoida riskiarvioiden kehitystä vuosien välillä."""
        
        status = f"Dokumentit ladattu: Kymenlaakson riskiarvio {doc1['year']} ja {doc2['year']}"
        return context, status, gr.update(visible=True)
    
    def respond_with_docs(message, chat_history, doc_context):
        if not message.strip():
            return "", chat_history
        
        if not doc_context:
            chat_history.append({"role": "user", "content": message})
            chat_history.append({"role": "assistant", "content": "Dokumentteja ei ole ladattu. Paina ensin 'Lataa riskiarvio-dokumentit' -nappia."})
            return "", chat_history
        
        # Build conversation history string for context
        history_str = ""
        for msg in chat_history:
            role = "Käyttäjä" if msg["role"] == "user" else "Assistentti"
            history_str += f"{role}: {msg['content']}\n\n"
        
        # Always include document context in every request
        full_prompt = f"""{doc_context}

--- KESKUSTELUHISTORIA ---
{history_str if history_str else "(Ei aiempaa keskustelua)"}

--- UUSI KYSYMYS ---
Käyttäjä: {message}

Vastaa käyttäjän kysymykseen yllä olevien dokumenttien ja keskusteluhistorian perusteella."""
        
        # Get response from Gemini (default model for document chat)
        bot_response = llm_client.chat(full_prompt, history=None, model="gemini")
        
        if bot_response is None:
            bot_response = "Pahoittelut, kohtasin virheen vastausta luodessa. Yritä uudelleen."
        
        # Add messages to chat history
        chat_history.append({"role": "user", "content": message})
        chat_history.append({"role": "assistant", "content": bot_response})
        
        return "", chat_history
    
    def clear_doc_chat():
        return []
    
    # Event handlers for document chat
    load_docs_btn.click(
        load_documents,
        outputs=[doc_context_state, doc_status, doc_chat_section]
    )
    doc_msg_input.submit(
        respond_with_docs,
        [doc_msg_input, doc_chatbot, doc_context_state],
        [doc_msg_input, doc_chatbot]
    )
    doc_send_btn.click(
        respond_with_docs,
        [doc_msg_input, doc_chatbot, doc_context_state],
        [doc_msg_input, doc_chatbot]
    )
    doc_clear_btn.click(clear_doc_chat, None, doc_chatbot)


def build_delfoi_panel_tab(demo):
    """Build the Delfoi-paneeli tab with AI expert panel discussion functionality"""
    
    gr.Markdown("## Delfoi-paneeli - Tekoälyasiantuntijat keskustelevat")
    gr.Markdown("""
    Valitse aihe ja anna tekoälyasiantuntijapaneelin keskustella siitä. Eri mallit tuovat 
    oman näkökulmansa ja käyvät vuoropuhelua keskenään. Viimeisellä kierroksella panelistit pyrkivät konsensukseen.
    """)
    
    with gr.Row():
        with gr.Column(scale=2):
            panel_topic = gr.Textbox(
                label="Keskustelun aihe",
                placeholder="Esim: Tekoälyn vaikutus työelämään vuonna 2025",
                lines=2
            )
            panel_context = gr.Textbox(
                label="Taustatieto (valinnainen)",
                placeholder="Anna kontekstia tai taustamateriaalia keskusteluun...",
                lines=4
            )
        
        with gr.Column(scale=1):
            # Get available models for panel
            available_models = [config.name for config in llm_client.get_available_models()]
            
            panel_models = gr.CheckboxGroup(
                choices=[(config.display_name, config.name) 
                        for config in llm_client.get_all_models()],
                value=available_models[:4] if len(available_models) >= 4 else available_models,
                label="Valitse panelistit (2-4 mallia)",
                interactive=True
            )
            
            panel_rounds = gr.Slider(
                minimum=1,
                maximum=5,
                value=3,
                step=1,
                label="Keskustelukierrokset",
                info="Kuinka monta kierrosta panelistit keskustelevat"
            )
            
            panel_temperature = gr.Slider(
                minimum=0.1,
                maximum=1.0,
                value=0.7,
                step=0.1,
                label="Lampotila (temperature)",
                info="Korkeampi = luovempi, matalampi = johdonmukaisempi"
            )
            
            panel_max_tokens = gr.Slider(
                minimum=256,
                maximum=2048,
                value=1024,
                step=256,
                label="Maksimi tokenit per vastaus",
                info="Pidempi = yksityiskohtaisemmat vastaukset"
            )
    
    # Persona customization section
    with gr.Accordion("Muokkaa panelistien profiileja", open=False):
        gr.Markdown("Voit muokata kunkin mallin roolia ja näkökulmaa keskustelussa.")
        
        # Default personas from llm_client.py
        persona_gemini = gr.Textbox(
            label="Gemini - profiili",
            value="Analyyttinen asiantuntija, joka keskittyy faktoihin ja loogiseen analyysiin.",
            lines=2
        )
        persona_claude = gr.Textbox(
            label="Claude - profiili",
            value="Filosofinen ajattelija, joka pohtii asiaa monesta näkökulmasta ja nostaa esiin eettisiä kysymyksiä.",
            lines=2
        )
        persona_gemma = gr.Textbox(
            label="Gemma - profiili",
            value="Käytännönläheinen asiantuntija, joka tuo esiin konkreettisia esimerkkejä ja ratkaisuja.",
            lines=2
        )
        persona_oss = gr.Textbox(
            label="OSS-120B - profiili",
            value="Kriittinen arvioija, joka kyseenalaistaa oletuksia ja etsii vaihtoehtoisia näkökulmia.",
            lines=2
        )
    
    # Loading indicator
    panel_loading = gr.HTML(visible=False, value='<div style="display: flex; align-items: center; gap: 10px; padding: 10px; background: #f0f7ff; border-radius: 8px;"><div style="width: 20px; height: 20px; border: 3px solid #3b82f6; border-top-color: transparent; border-radius: 50%; animation: spin 1s linear infinite;"></div><span>Paneeli keskustelee... Tämä voi kestää hetken.</span></div><style>@keyframes spin { to { transform: rotate(360deg); } }</style>')
    
    with gr.Row():
        start_panel_btn = gr.Button("Aloita paneelikeskustelu", variant="primary", scale=2)
        reset_panel_btn = gr.Button("Tyhjennä keskustelu", variant="secondary", scale=1)
    
    panel_status = gr.Textbox(label="Tila", interactive=False)
    
    # Panel discussion display
    panel_discussion = gr.Markdown(label="Paneelikeskustelu", value="")
    
    # Summary section
    with gr.Row():
        generate_summary_btn = gr.Button("Luo yhteenveto", variant="secondary")
    panel_summary = gr.Markdown(label="Yhteenveto", value="")
    
    # State to track if discussion has been run
    panel_has_discussion = gr.State(value=False)
    
    def run_delphi_panel(topic, context, selected_models, rounds, temperature, max_tokens,
                         p_gemini, p_claude, p_gemma, p_oss):
        if not topic.strip():
            yield gr.update(visible=False), "Anna keskustelun aihe.", "", False
            return
        
        if len(selected_models) < 2:
            yield gr.update(visible=False), "Valitse vähintään 2 panelistia.", "", False
            return
        
        if len(selected_models) > 4:
            yield gr.update(visible=False), "Valitse enintään 4 panelistia.", "", False
            return
        
        # Build custom personas dict
        custom_personas = {
            "gemini": p_gemini,
            "claude": p_claude,
            "gemma": p_gemma,
            "oss": p_oss
        }
        
        # Show loading indicator
        yield gr.update(visible=True), "Aloitetaan paneelikeskustelu...", "", False
        
        # Reset discussion
        delphi_panel.reset_discussion()
        
        discussion_html = f"## Paneelikeskustelu: {topic}\n\n"
        
        if context.strip():
            discussion_html += f"**Taustatieto:**\n{context}\n\n---\n\n"
        
        try:
            for round_num in range(1, int(rounds) + 1):
                is_last = (round_num == int(rounds))
                round_label = f"Kierros {round_num}" + (" (Konsensus)" if is_last else "")
                discussion_html += f"### {round_label}\n\n"
                
                for entry in delphi_panel.run_round(
                    topic=topic,
                    panelists=selected_models,
                    initial_context=context,
                    temperature=temperature,
                    max_tokens=int(max_tokens),
                    round_num=round_num,
                    total_rounds=int(rounds),
                    custom_personas=custom_personas
                ):
                    discussion_html += f"**{entry['display_name']}:**\n\n{entry['response']}\n\n---\n\n"
                    # Yield intermediate results to show progress
                    yield gr.update(visible=True), f"Kierros {round_num}/{int(rounds)} - {entry['display_name']} vastasi...", discussion_html, True
            
            # Hide loading indicator when done
            final_status = f"Paneelikeskustelu valmis! {len(selected_models)} panelistia, {int(rounds)} kierrosta."
            yield gr.update(visible=False), final_status, discussion_html, True
            
        except Exception as e:
            yield gr.update(visible=False), f"Virhe: {str(e)}", discussion_html, False
    
    def generate_panel_summary(topic, has_discussion):
        if not has_discussion:
            return "Ei keskustelua yhteenvedolle. Aloita ensin paneelikeskustelu."
        
        summary = delphi_panel.generate_summary(topic)
        if summary:
            return f"## Yhteenveto\n\n{summary}"
        else:
            return "Yhteenvedon luominen epäonnistui."
    
    def reset_delphi_panel():
        delphi_panel.reset_discussion()
        return gr.update(visible=False), "Keskustelu tyhjennetty.", "", "", False
    
    # Event handlers
    start_panel_btn.click(
        run_delphi_panel,
        inputs=[panel_topic, panel_context, panel_models, panel_rounds, 
               panel_temperature, panel_max_tokens,
               persona_gemini, persona_claude, persona_gemma, persona_oss],
        outputs=[panel_loading, panel_status, panel_discussion, panel_has_discussion]
    )
    
    generate_summary_btn.click(
        generate_panel_summary,
        inputs=[panel_topic, panel_has_discussion],
        outputs=[panel_summary]
    )
    
    reset_panel_btn.click(
        reset_delphi_panel,
        outputs=[panel_loading, panel_status, panel_discussion, panel_summary, panel_has_discussion]
    )
