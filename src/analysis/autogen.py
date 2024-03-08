import openai

# Your OpenAI API key
openai.api_key = 'your_api_key_here'

def generate_test_for_function(func_name, func_description):
    # Construct the prompt
    prompt = f"Write a pytest test case for a function named `{func_name}` that {func_description}"
    
    # Call the ChatGPT API
    response = openai.Completion.create(
        engine="text-davinci-003",  # Adjust based on available engines
        prompt=prompt,
        temperature=0.7,
        max_tokens=150,
        top_p=1.0,
        frequency_penalty=0.0,
        presence_penalty=0.0
    )
    
    # Extract the test case from the response
    test_case = response.choices[0].text.strip()
    return test_case

# Example usage
func_name = "add_numbers"
func_description = "takes two integers as input and returns their sum"
test_case = generate_test_for_function(func_name, func_description)
print(test_case)
