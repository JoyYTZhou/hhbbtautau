import os, argparse
from mistralai import Mistral

api_key = os.environ["MISTRAL_API_KEY"]

def read_fi(fname):
    with open(fname, 'r') as f:
        return f.read()

def wri_fi(fname, content):
    with open(fname, 'w') as f:
        f.write(content)

def main():
    parser = argparse.ArgumentParser(description='Debug a program using Mistral AI')
    parser.add_argument('output_file', type=str, help='Path to the output file')
    parser.add_argument('--solutions', type=str, default='solutions.txt', help='Path to the output solutions file.')

    args = parser.parse_args()
    model = 'mistral-large-latest'

    client = Mistral(api_key=api_key)

    output_content = read_fi(args.output_file)

    user_input = input("Please describe the issue you want to address: ")
    
    message = f"Here is the output from the program:\n{output_content}\n\nUser input:\n{user_input}\n\nPlease provide a solution."

    chat_response = client.chat.complete(
        model = model,
        messages = [
            {
                "role": "user",
                "content": message,
            },
        ]
    )

    solution = chat_response.choices[0].message.content

    wri_fi(args.solutions, solution)
    print(f"Solution written to {args.solutions}")

if __name__ == '__main__':
    main()