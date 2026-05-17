lines.append(json.dumps(obj))

combined = "\n".join(lines)
    return combined.encode("utf-8")