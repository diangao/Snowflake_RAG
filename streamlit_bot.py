import streamlit as st
from streamlit_extras.let_it_rain import rain
#from snowflake.snowpark.context import get_active_session
from datetime import date
from snowflake.core import Root
import json

#from dotenv import load_dotenv
from snowflake.snowpark.session import Session
import os

#load_dotenv()

connection_params = {
    "account": st.secrets["snowflake"]["account"],
    "user": st.secrets["snowflake"]["user"],
    "password": st.secrets["snowflake"]["password"],
    "role": st.secrets["snowflake"]["role"],
    "database": st.secrets["snowflake"]["database"],
    "schema": st.secrets["snowflake"]["schema"],
    "warehouse": st.secrets["snowflake"]["warehouse"]
}

session = Session.builder.configs(connection_params).create()

# -------------------------------
# Session State Initialization
# -------------------------------

def init_user_session():
    session_vars = {
        "user_logged_in": False,
        "current_user": None,
        "pets": [],
        "current_pet": None,
        "messages": {},
        "clinical_history": {},
        "daily_check_ins": {},
        "current_view": "Current Pet",
        "model_name": 'mistral-large2'
    }
    
    
    for var, default in session_vars.items():
        if var not in st.session_state:
            st.session_state[var] = default



# -------------------------------
# Navigation Functions
# -------------------------------

def change_view(new_view):
    st.session_state.current_view = new_view
    st.rerun()

def handle_logout():
    for key in ['user_logged_in', 'current_user', 'pets', 'messages', 
                'clinical_history', 'daily_check_ins', 'current_pet']:
        if key in st.session_state:
            st.session_state[key] = None if key in ['current_user', 'current_pet'] else (
                {} if key in ['messages', 'clinical_history', 'daily_check_ins'] else (
                    [] if key == 'pets' else False))
    st.session_state.current_view = "Current Pet"
    st.rerun()


# -------------------------------
# Call RAG Service
# -------------------------------
CORTEX_SEARCH_DATABASE = "ANIMAL_DATA"
CORTEX_SEARCH_SCHEMA = "PUBLIC"
CORTEX_SEARCH_SERVICE = "exact_type_search"
CORTEX_SEARCH_SERVICE_CONDITION = "condition_match_search"



root = Root(session)    
NUM_CHUNKS = 5
slide_window = 7
COLUMNS=["chunk", "relative_path", "pet_type"]

svc = root.databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[CORTEX_SEARCH_SERVICE_CONDITION]

def get_similar_chunks_search_service(query, type):
    # 构建过滤条件
    filter_condition = {
      "@or": [
          {"@eq": {"pet_type": type}}, {"@eq": {"pet_type": "Undefined"}}
      ]
    }
    
    try:
        response = svc.search(query, COLUMNS, limit=NUM_CHUNKS, filter=filter_condition)
        
        return json_response
    
    except Exception as e:
        st.error(f"Error occurred while querying the service: {str(e)}")
        return {"results": []}
        

def get_chat_history():
    
    chat_history = []
    
    start_index = max(0, len(st.session_state.messages[st.session_state.current_pet]) - slide_window)
    for i in range (start_index , len(st.session_state.messages[st.session_state.current_pet]) -1):
         chat_history.append(st.session_state.messages[st.session_state.current_pet][i])

    return chat_history

def summarize_question_with_history(chat_history, question):
# To get the right context, use the LLM to first summarize the previous conversation
# This will be used to get embeddings and find similar chunks in the docs for context

    prompt = f"""
        Based on the chat history below and the question, generate a query that extend the question
        with the chat history provided. The query should be in natual language. 
        Answer with only the query. Do not add any explanation.
        
        <chat_history>
        {chat_history}
        </chat_history>
        <question>
        {question}
        </question>
        """
    sumary = session.sql(
        "SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?)", 
        params=[st.session_state.model_name, prompt]
        ).collect()[0]['SNOWFLAKE.CORTEX.COMPLETE(?, ?)']


    sumary = sumary.replace("'", "")

    return sumary

def rewrite_query(question):
    """
    Use the LLM to rewrite the original query into a more contextually aligned and refined query.
    """
    prompt = f"""
        Rewrite the following question to make it more formal, specific, and aligned to retrieve relevant information from a medical database for pets.
        The rewritten query should focus on key terms and provide clarity for searching.
        Answer with only the rewritten query. Do not add any explanation.

        <question>
        {question}
        </question>
    """
    rewritten_query = session.sql(
        "SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?)", 
        params=[st.session_state.model_name, prompt]
    ).collect()[0]['SNOWFLAKE.CORTEX.COMPLETE(?, ?)']

    # Remove any extra characters like quotes that might break downstream tasks
    rewritten_query = rewritten_query.replace("'", "")

    return rewritten_query


def create_prompt (myquestion):
    pet_info = get_pet_info()
    type = pet_info['TYPE']
    # st.write(type)
    chat_history = get_chat_history()   
    clinical_history = []
    daily_checkins = []
    result = session.sql(
                'SELECT date, notes FROM clinical_history WHERE pet_id = ?', 
                (st.session_state.current_pet,)
                ).collect()
    for row in result:
        clinical_history.append((row['DATE'],row['NOTES']))
    result = session.sql(
                'SELECT date,condition,notes FROM daily_check_ins WHERE pet_id = ?', 
                (st.session_state.current_pet,)
                ).collect()
    for row in result:
        daily_checkins.append((row['DATE'],row['CONDITION'],row['NOTES']))


    if chat_history!= []: #There is chat_history, so not first question
        question_summary = summarize_question_with_history(chat_history, myquestion)
        # prompt_context =  get_similar_chunks_search_service(question_summary, type)
        prompt_context =  get_similar_chunks_search_service(rewrite_query(question_summary), type)
    else:
        # prompt_context = get_similar_chunks_search_service(myquestion, type)
        prompt_context = get_similar_chunks_search_service(rewrite_query(myquestion), type) #First question when using history
        
        # 将搜索结果显示到前端
        # st.subheader("Search Results")
        # st.write(prompt_context)
        
    prompt = f"""
           You are an expert chat assistance to offer professional suggestions about pet daily care and disease related problems.
           You need to extract information from the CONTEXT provided between <context> and </context> tags.
           You offer a chat experience considering the information included in the CHAT HISTORY provided between <chat_history> and </chat_history> tags.
           You need to take the information included in the CLINICAL HISTORY provided between <clinical_history> and </clinical_history> tags.
           You need to take the information included in the DAILY CHECKINS provided between <daily_ckeckins> and </daily_checkins> tags.
           When ansering the question contained between <question> and </question> tags be concise and do not hallucinate. 
           If you don't have the information, give a general idea and mention you are not sure.
           
           Do not mention the CONTEXT used in your answer.
           Do not mention the CHAT HISTORY used in your asnwer.

           - Explain medical terms or complex issues in language that anyone without medical knowledge can understand.
           - Provide ACTIONABLE ADVICE where applicable.
           - Keep the response conversational and easy to understand.
           - Be empathetic and reassuring when addressing concerns.

           Only anwer the question if you can extract it from the CONTEXT provideed.
           
           <chat_history>
           {chat_history}
           </chat_history>
           <clinical_history>
           {clinical_history}
           </clinical_history>
           <daily_checkins>
           {daily_checkins}
           </daily_checkins>
           <context>          
           {prompt_context}
           </context>
           <question>  
           {myquestion}
           </question>
           Answer: 
           """
           
    # json_data = json.loads(prompt_context)

    # relative_paths = set(item['relative_path'] for item in json_data['results'])
    relative_paths = set(item['relative_path'] for item in prompt_context['results'])

    return prompt, relative_paths


def answer_question(myquestion):

    prompt, relative_paths =create_prompt (myquestion)
    response = session.sql(
        "SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?)", 
        params=[st.session_state.model_name, prompt]
        ).collect()[0]['SNOWFLAKE.CORTEX.COMPLETE(?, ?)']
    return response, relative_paths


def assign_pet_type(chunk: str) -> str:
        # Use LLM to determine pet type
        prompt = f"""
        Given the following text, classify it as one of the following categories:
        - Large Cat
        - Small Cat
        - Large Dog
        - Small Dog
        - Undefined

        You must ONLY respond with one of these exact categories (no additional text or explanation). If the text cannot be clearly classified, respond with 'Undefined'.

        Text:
        {chunk}
        """

        pet_type = session.sql("SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?)", params=['mistral-large2', prompt]).collect()[0][0]
        # Validate response and default to 'Undefined' if not in expected values
        return pet_type

# -------------------------------
# Registration & Login Functions
# -------------------------------

def register_user():
    st.subheader("User Registration")
    with st.form("registration_form", clear_on_submit=True):
        new_username = st.text_input("Choose a username:")
        new_password = st.text_input("Choose a password:", type="password")
        submitted = st.form_submit_button("Register")

        if submitted:
            result = session.sql("SELECT username FROM users").collect()
            all_usernames = [row['USERNAME'] for row in result]
            if not new_username or not new_password:
                st.error("Please fill out both username and password!")
            elif new_username in all_usernames:
                st.error("Username already exists. Please choose a different one.")
            else:
                session.sql("INSERT INTO users (username, password) VALUES (?, ?)", 
                          (new_username, new_password)).collect()
                st.success("Registration successful! You can now log in.")
                change_view("Login")

def login_user():
    result = session.sql("SELECT username,password FROM users").collect()
    all_pairs = {row['USERNAME']:row['PASSWORD'] for row in result}

    st.subheader("User Login")
    with st.form("login_user_form", clear_on_submit=True):
        username = st.text_input("Username:")
        password = st.text_input("Password:", type="password")
        submitted = st.form_submit_button("Login")

        if submitted:
            if username in all_pairs.keys() and all_pairs.get(username,'') == password:
                st.session_state.user_logged_in = True
                st.session_state.current_user = session.sql("select ID from users where username=?",
                                                          (username,)).collect()[0]['ID']
                
                # Check if user has any pets
                result = session.sql("SELECT COUNT(*) as pet_count FROM pets WHERE user_id = ?",
                                   (st.session_state.current_user,)).collect()
                has_pets = result[0]['PET_COUNT'] > 0
                
                if has_pets:
                    # If user has pets, load them and set current pet to the first one
                    pets_result = session.sql("SELECT id FROM pets WHERE user_id = ?",
                                            (st.session_state.current_user,)).collect()
                    st.session_state.pets = [row['ID'] for row in pets_result]
                    st.session_state.current_pet = st.session_state.pets[0]
                    change_view("Current Pet")
                else:
                    # If no pets, direct to add pet page
                    change_view("Add Another Pet")
                st.rerun()
            else:
                st.error("Invalid username or password")


# -------------------------------
# Pet Management Functions
# -------------------------------

def add_pet():
    with st.form("add_pet_form", clear_on_submit=True):
        st.subheader("Add a Pet :dog: :cat:")
        pet_name = st.text_input("Pet's Name:")
        pet_breed = st.text_input("Breed:")
        pet_gender = st.selectbox("Gender:", ["Male", "Female"])
        pet_birthday = st.date_input("Birthday:")
        
        today = date.today()
        age = today.year - pet_birthday.year - ((today.month, today.day) < 
              (pet_birthday.month, pet_birthday.day))
        submitted = st.form_submit_button("Add Pet")
        if submitted:
            if not pet_name or not pet_breed:
                st.error("Please fill out all required fields!")
                return
            pet_type = assign_pet_type(pet_breed)
            # st.write(pet_type)
            if pet_type=='Undefined':
                st.error("Try to provide more information about the breed!")
                return
            result = session.sql("SELECT name FROM pets WHERE user_id = ?", 
                               (st.session_state.current_user,)).collect()
            existing_names = [row['NAME'] for row in result]
            
            if pet_name in existing_names:
                st.error("You already have a pet with this name!")
                return
                
            session.sql(
                """INSERT INTO pets (user_id, name, breed, type, gender, age) 
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (st.session_state.current_user, pet_name, pet_breed, pet_type, pet_gender, age)
            ).collect()
            # st.write(f"Debugging Inputs:")
            # st.write(f"user_id: {st.session_state.current_user}")
            # st.write(f"name: {pet_name}")
            # st.write(f"breed: {pet_breed}")
            # st.write(f"type: {pet_type}")
            # st.write(f"gender: {pet_gender}")
            # st.write(f"age: {age}")
            inserted_id = session.sql(
                """SELECT id FROM pets 
                   WHERE user_id = ? AND name = ? 
                   ORDER BY id DESC LIMIT 1""",
                (st.session_state.current_user, pet_name)
            ).collect()[0]['ID']
            
            if inserted_id not in st.session_state.pets:
                st.session_state.pets.append(inserted_id)
            st.session_state.current_pet = inserted_id
            st.session_state.messages[inserted_id] = []
            st.session_state.clinical_history[inserted_id] = []
            st.session_state.daily_check_ins[inserted_id] = []
            
            st.success(f"Added {pet_name} successfully!")
            st.balloons()
            change_view("Current Pet")

def switch_pet():
    result = session.sql(
        "SELECT name, id FROM pets WHERE user_id=?",
        (st.session_state.current_user,)
    ).collect()
    
    pet_names = {row['NAME']:row['ID'] for row in result}
    if pet_names:
        selected_pet = st.selectbox(
            "Select a pet to manage:",
            pet_names.keys(),
            index=0 if st.session_state.current_pet not in pet_names.values() else 
                  list(pet_names.values()).index(st.session_state.current_pet)
        )
        
        if st.session_state.current_pet != pet_names[selected_pet]:
            st.session_state.current_pet = pet_names[selected_pet]
            st.rerun()
    else:
        st.write("No pets available. Please add a pet.")


def logout():
    st.session_state.user_logged_in = False
    st.session_state.current_user = None
    st.session_state.pets = []
    st.session_state.messages = {}
    st.session_state.clinical_history = {}
    st.session_state.daily_check_ins = {}
    st.session_state.current_pet = None
    st.session_state.current_view = "Current Pet"
    st.success("You have been logged out.")


# --------------------------------
# Additional Pages
# --------------------------------
def record_clinical_history():
    """
    Allows the user to record clinical history entries for the selected pet.
    Shows a message if no pets are available.
    """
    st.subheader("Record Clinical History")
    
    # Check if there are any pets
    result = session.sql(
        "SELECT COUNT(*) as pet_count FROM pets WHERE user_id = ?",
        (st.session_state.current_user,)
    ).collect()
    pet_count = result[0]['PET_COUNT']
    
    if pet_count == 0:
        st.warning("⚠️ You haven't added any pets yet! Please add a pet first before recording clinical history.")
        if st.button("Go to Add Pet"):
            change_view("Add Another Pet")
        return
        
    current_pet = st.session_state.current_pet
    pet_info = get_pet_info()
    
    st.write(f"Recording clinical history for **{pet_info['NAME']}**")
    with st.form("clinical_history_form", clear_on_submit=True):
        visit_date = st.date_input("Visit Date:", value=date.today())
        notes = st.text_area("Clinical Notes:", "")
        submitted = st.form_submit_button("Save History")

        if submitted:
            if notes:
                record = {
                    "date": visit_date,
                    "notes": notes,
                }
                st.session_state.clinical_history.get(current_pet,[]).append(record)
                session.sql(
                    "INSERT INTO clinical_history (pet_id, date, notes) VALUES (?, ?, ?)", 
                    (st.session_state.current_pet, record['date'], record['notes'])).collect()
                st.success("Clinical history saved!")
            else:
                st.error("Please enter some notes.")

    # Display existing history
    st.write("### Clinical History Records")
    result = session.sql(
                'SELECT date, notes FROM clinical_history WHERE pet_id = ?', 
                (st.session_state.current_pet,)
                ).collect()
    for row in result:
        st.write(f"**Date**: {row['DATE']} - **Notes**: {row['NOTES']}")

def daily_check_in():
    """
    Allows the user to do a daily or regular check-in for the selected pet's condition.
    Shows a message if no pets are available.
    """
    st.subheader("Daily/Regular Check-In")
    
    # Check if there are any pets
    result = session.sql(
        "SELECT COUNT(*) as pet_count FROM pets WHERE user_id = ?",
        (st.session_state.current_user,)
    ).collect()
    pet_count = result[0]['PET_COUNT']
    
    if pet_count == 0:
        st.warning("⚠️ You haven't added any pets yet! Please add a pet first before doing a daily check-in.")
        if st.button("Go to Add Pet"):
            change_view("Add Another Pet")
        return
        
    pet_info = get_pet_info()
    st.write(f"Daily check-in for **{pet_info['NAME']}**")
    with st.form("daily_check_in_form", clear_on_submit=True):
        check_date = st.date_input("Check-In Date:", value=date.today())
        condition = st.radio(
            "How is your pet's condition today?",
            ["Excellent", "Good", "Fair", "Poor"],
            index=1
        )
        notes = st.text_area("Additional Notes:", "")
        submitted = st.form_submit_button("Save Check-In")
        if submitted:
            record = {
                "date": check_date,
                "condition": condition,
                "notes": notes,
            }
            st.session_state.daily_check_ins.get(st.session_state.current_pet,[]).append(record)
            # Execute the SQL insert without displaying the result
            session.sql(
                "INSERT INTO daily_check_ins (pet_id, date, condition, notes) VALUES (?, ?, ?, ?)", 
                (st.session_state.current_pet, record['date'], record['condition'],record['notes'])
            ).collect()
            st.success("Daily check-in saved!")
            
    # Display existing check-in data
    st.write("### Check-In History")
    result = session.sql("select date, condition, notes from daily_check_ins where pet_id=?",(st.session_state.current_pet,)).collect()
    for row in result:
        st.write(f"**Date**: {row['DATE']} - **Condition**: {row['CONDITION']} - **Notes**: {row['NOTES']}")
# --------------------------------
# Bubbles effect
# --------------------------------
def add_bubbles():
    if "bubbles_shown" not in st.session_state:
        rain(
            emoji="🌈",  # Rainbow emoji
            font_size=54,
            falling_speed=5,
            animation_length=5,
        )
        st.session_state.bubbles_shown = True


# --------------------------------
# Configuration (Sidebar) 
# --------------------------------
def config_options():
    st.sidebar.button("Logout", on_click=logout)




def get_pet_info():
    if st.session_state.current_pet is not None:
        result = session.sql(
            """SELECT name, breed, type, gender, age 
               FROM pets WHERE id = ?""",
            (st.session_state.current_pet,)
        ).collect()
        return result[0] if result else None
    return None

# --------------------------------
# Main Application
# --------------------------------
def main():
    
    init_user_session()
    st.title("FurWell :cat: :dog:")
    add_bubbles()

    if not st.session_state.user_logged_in:
        choice = st.radio("Please select:", ["Login", "Register"], horizontal=True)
        if choice == "Login":
            login_user()
        else:
            register_user()
    else:
        if st.sidebar.button("Logout"):
            handle_logout()
        
        view_options = [
            "Current Pet",
            "Add Another Pet",
            "Record Clinical History",
            "Daily Check In",
        ]
        
        selected_view = st.radio(
            "Choose what you want to do:",
            view_options,
            horizontal=True,
            index=view_options.index(st.session_state.current_view)
        )
        
        if selected_view != st.session_state.current_view:
            change_view(selected_view)
        
        if st.session_state.current_view == "Current Pet":
            st.subheader("Manage Your Pets :sparkles:")
            switch_pet()

            pet_info = get_pet_info()
            if pet_info:
                st.markdown(f"""
                    **Currently Managing:** {pet_info['NAME']}  
                    **Breed:** {pet_info['BREED']}
                    **Type:** {pet_info['TYPE']}  
                    **Gender:** {pet_info['GENDER']}  
                    **Age:** {pet_info['AGE']} years  
                """)

                st.write(
                    f"""Welcome! This application helps you manage {pet_info['NAME']}'s health 
                    and provides guidance on whether their condition requires immediate vet 
                    attention or can be handled at home."""
                )

                if st.session_state.current_pet not in st.session_state.messages.keys():
                    st.session_state.messages[st.session_state.current_pet] = []

                for message in st.session_state.messages[st.session_state.current_pet]:
                    with st.chat_message(message["role"]):
                        st.markdown(message["content"])

                if question := st.chat_input(f"Ask about {pet_info['NAME']}"):
                    st.session_state.messages[st.session_state.current_pet].append(
                        {"role": "user", "content": question}
                    )
                    with st.chat_message("user"):
                        st.markdown(question)
                    
                    with st.chat_message("assistant"):
                        message_placeholder = st.empty()
                        question = question.replace("'","")
                        with st.spinner("Thinking..."):
                            response, _ = answer_question(question)            
                            response = response.replace("'", "")
                            message_placeholder.markdown(response)
            
                    
                    st.session_state.messages[st.session_state.current_pet].append({"role": "assistant", "content": response})
                                

        elif st.session_state.current_view == "Add Another Pet":
            add_pet()
        elif st.session_state.current_view == "Record Clinical History":
            record_clinical_history()
        elif st.session_state.current_view == "Daily Check In":
            daily_check_in()

if __name__ == "__main__":
    main()