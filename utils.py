def extract_first_json(text):
    """
    Extract the first valid JSON object from a string.

    Args:
        text (str): The string that may contain JSON objects

    Returns:
        dict or None: The first valid JSON object found, or None if no valid JSON is found
    """
    # Look for patterns that might be JSON objects (text between curly braces)
    json_pattern = re.compile(r'\{(?:[^{}]|(?R))*\}')

    # Alternative approach without recursive pattern if your Python's re doesn't support it
    # This tries to find balanced curly braces, but isn't perfect for nested structures
    if not hasattr(re, 'Pattern'):  # Check if we're using older Python without Pattern class
        # Find all occurrences of text between curly braces
        open_braces = []
        close_braces = []
        in_string = False
        escape = False

        for i, char in enumerate(text):
            if char == '"' and not escape:
                in_string = not in_string
            elif not in_string:
                if char == '{':
                    open_braces.append(i)
                elif char == '}' and open_braces:
                    start = open_braces.pop(0)
                    potential_json = text[start:i + 1]
                    try:
                        result = json.loads(potential_json)
                        return result  # Return the first valid JSON object
                    except json.JSONDecodeError:
                        pass  # Not a valid JSON, continue searching

            escape = char == '\\' and not escape

        return None  # No valid JSON found

    # For newer Python versions with better regex support
    matches = json_pattern.finditer(text)
    for match in matches:
        potential_json = match.group(0)
        try:
            result = json.loads(potential_json)
            return result  # Return the first valid JSON object
        except json.JSONDecodeError:
            continue  # Not a valid JSON, try the next match

    return None  # No valid JSON found
