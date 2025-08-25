import argparse
import json


def convert_proxies_to_dict(file_path):
    # Initialize the dictionary with an empty proxy list
    proxy_dict = {"proxy": []}

    # Open and read the file
    with open(file_path, 'r') as file:
        # Process each line
        for line in file:
            # Strip whitespace and newline characters
            line = line.strip()

            # Skip empty lines
            if not line:
                continue

            # Parse the line (IP:PORT:USERNAME:PASSWORD)
            try:
                parts = line.split(':')
                if len(parts) == 4:
                    ip, port, username, password = parts
                    # Format into http://USERNAME:PASSWORD@IP:PORT
                    proxy_url = f"http://{username}:{password}@{ip}:{port}"
                    proxy_dict["proxy"].append(proxy_url)
            except Exception as e:
                print(f"Error processing line: {line}. Error: {e}")

    return proxy_dict


def main():
    # Set up command-line argument parsing
    parser = argparse.ArgumentParser(description='Convert proxy list file to dictionary format.')
    parser.add_argument('file_path', help='Path to the proxy list text file')
    parser.add_argument('--output', '-o', help='Output file path (optional, prints to console if not provided)')

    # Parse arguments
    args = parser.parse_args()

    # Convert the file to a dictionary
    proxy_dict = convert_proxies_to_dict(args.file_path)

    # Format the dictionary as JSON
    formatted_json = json.dumps(proxy_dict, indent=2)

    # Output result
    if args.output:
        with open(args.output, 'w') as out_file:
            out_file.write(formatted_json)
        print(f"Proxy dictionary written to {args.output}")
    else:
        print(formatted_json)


if __name__ == "__main__":
    main()