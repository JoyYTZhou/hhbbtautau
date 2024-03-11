import os
import inspect
import importlib.util
from openai import OpenAI

api_key_file = '.api_key.txt'
with open(api_key_file, 'r') as file:
    api_key = file.read().strip()

client = OpenAI(api_key=api_key)

def generate_test_case_with_openai(function_name, module_path):
    spec = importlib.util.spec_from_file_location("module.name", os.path.join(os.getcwd(), module_path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    function = getattr(module, function_name)
    source_code = inspect.getsource(function)

    prompt = f"I want to test the Python function below:\n{source_code}\nCan you suggest a test case for this function?"

    response = openai.Completion.create(
        engine="gpt-4-turbo-preview",
        prompt=prompt,
        temperature=0.5,
        max_tokens=100
    )

    return response.choices[0].text.strip()

if __name__ == '__main__':
    test_case = generate_test_case_with_openai('sync_files', 'helper.py')
    print(test_case)
