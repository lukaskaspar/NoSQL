import json
from pathlib import Path

INPUT_FILE = "yelp_academic_dataset_user.json"
OUTPUT_FILE = "yelp_academic_dataset_user.json"

#INPUT_FILE = "yelp_academic_dataset_review.json"
#OUTPUT_FILE = "yelp_academic_dataset_review.json"

#INPUT_FILE = "yelp_academic_dataset_business.json"
#OUTPUT_FILE = "yelp_academic_dataset_business.json"

#BUSINESS: (150346, 14)
#REVIEW: (500000, 9)
#USER: (100000, 22)"

MAX_RECORDS = 100000


def detect_json_type(file_path: str) -> str:
    with open(file_path, "r", encoding="utf-8") as f:
        while True:
            ch = f.read(1)
            if not ch:
                raise ValueError("Soubor je prázdný.")
            if not ch.isspace():
                if ch == "[":
                    return "json_array"
                elif ch == "{":
                    return "json_lines"
                else:
                    raise ValueError("Nepodařilo se rozpoznat formát JSON.")


def shrink_json_lines(input_file: str, output_file: str, max_records: int) -> None:
    count = 0
    with open(input_file, "r", encoding="utf-8") as fin, open(output_file, "w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            fout.write(line + "\n")
            count += 1
            if count >= max_records:
                break
    print(f"Hotovo. Uloženo {count} záznamů do {output_file}")


def shrink_json_array(input_file: str, output_file: str, max_records: int) -> None:
    with open(input_file, "r", encoding="utf-8") as fin:
        data = json.load(fin)

    if not isinstance(data, list):
        raise ValueError("JSON není pole objektů.")

    smaller = data[:max_records]

    with open(output_file, "w", encoding="utf-8") as fout:
        json.dump(smaller, fout, ensure_ascii=False, indent=2)

    print(f"Hotovo. Uloženo {len(smaller)} záznamů do {output_file}")


def main():
    file_type = detect_json_type(INPUT_FILE)
    print(f"Rozpoznaný typ: {file_type}")

    if file_type == "json_lines":
        shrink_json_lines(INPUT_FILE, OUTPUT_FILE, MAX_RECORDS)
    elif file_type == "json_array":
        shrink_json_array(INPUT_FILE, OUTPUT_FILE, MAX_RECORDS)

    size_mb = Path(OUTPUT_FILE).stat().st_size / (1024 * 1024)
    print(f"Velikost výsledného souboru: {size_mb:.2f} MB")


if __name__ == "__main__":
    main()