import pytesseract
from PIL import Image
import re
import logging
import datetime


def process_image_from_stream(image_stream, index):
    try:
        image = Image.open(image_stream)
        logging.info(f"image {index} successfully opened. mode: {image.mode}")
        
        rotations = [0, 180]  
        text = ''
        for rotation in rotations:
            try:
                if rotation == 0:
                    current_text = pytesseract.image_to_string(image, timeout=30)
                else:
                    rotated_image = image.rotate(rotation, expand=True)
                    current_text = pytesseract.image_to_string(rotated_image, timeout=30)
                    rotated_image.close()
                    
                logging.info(f"rotation {rotation} degrees text:\n{current_text}")
                
                if "Take Out" in current_text or "Ordered:" in current_text:
                    text = current_text
                    logging.info(f"found valid receipt text at {rotation} degrees rotation")
                    break
                    
            except Exception as e:
                logging.error(f"error processing rotation {rotation} for image {index}: {str(e)}")
                if rotation != 0:
                    try:
                        rotated_image.close()
                    except:
                        pass

        if not text:
            logging.info(f"image {index} is invalid (no valid text found in any rotation)")
            image.close()
            return []
            
        parsed_data = parse_text(text)
        if not parsed_data:
            logging.warning(f"no data found from image {index}")
        image.close()
        return parsed_data
    except Exception as e:
        logging.error(f"error processing image {index}: {str(e)}", exc_info=True)
        try:
            image.close()
        except:
            pass
        return []
    

# 3 steps to clean up menu_item 
def clean_menu_item(item):
    patterns = [
        (r'\s*\$\s*\d+(?:[.,]\d{2})?', ''),  # Remove price artifacts
        (r'\s*["\']+\s*', ''), # Remove quotes with surrounding spaces
        (r'\s*[.,]*\s*00$', ''),  # Remove trailing "00"
        (r'\s*[.,]*$', ''),  # Remove trailing punctuation
        (r'\s*[-|]+\s*$', ''),  # Remove trailing dashes/bars
        (r'[=»|?<>#¥©+—;:%\\!_,]+', ''),  # Remove special characters
        (r'\s*\([^)]*\)', ''),  # Remove parentheses content
        (r'\b[A-Za-z]+\s*[.,]\b', ''),  # Remove stray letters with punctuation
        (r'\s*(?:slice|Slice)(?:\s|$)', ''),  # Remove "slice/Slice"
        (r'\s*\b(?:Loaf|loaf)\b', ''),  # Remove "Loaf/loaf"
        (r'\s*\[.*?\]', ''),  # Remove bracketed content
        (r'\s*\b(?:Ww|Ly)\b', '100% WW'),  # Standardize WW
        (r'\s+[A-Za-z]{1,2}$', ''),  # Remove trailing 1-2 letter suffixes
        (r'\s*\b(?:Ta|Tw|Ao|Cai|Si|Bi|Mi|Nb|Os|Ee|In|Of|On|A|I|Q|B|J|N|O|R|S|U|W|X)\b', ''),  # Remove garbage tokens
        (r'\s*\d+\s*(?:Oz|OZ|oz)?$', ''),  # Remove size indicators
        (r'\s*[)}\\*]+$', ''),  # Remove trailing special chars
        (r'\s*[\'"`]+', ''),  # Remove any remaining quotes
        (r'\s*\b(?:Out|Res|Ee|Wee)\b\s*$', ''),  # Remove common trailing artifacts
        (r'\s*\b(?:Jen|Cae|Le|Tw)\b', ''),  # Remove more garbage tokens
        (r'\s{2,}', ' '),  # Normalize spaces
        (r'(?<=\w)\]', ''),  # Remove ] when attached to word
        (r'\s+$', ''),  # Remove trailing spaces
    ]
    
    cleaned = item.strip()
    for pattern, replacement in patterns:
        cleaned = re.sub(pattern, replacement, cleaned)
    
    return cleaned.strip()

def standardize_menu_item(item):
    standard_products = {
        "Bluberry": "Blueberry",
        "Seasonallseasonal": "Seasonal",
        "Yegan": "Vegan",
        "Whcc": "WWCC",
        "Wncc": "WWCC",
        "100 Ww": "100% WW",
        "Ww100": "100% WW",
        "Levain.00": "Levain",
        "Croissantf": "Croissant",
        "Chocolat": "Chocolate",
        "Pumpernicke": "Pumpernickel",
        "Veoa": "Vegan",
        "PAC00": "PAC",
        "Xl Ka": "XL",
        "Xl": "XL",
        "Crx": "Croissant",
        "Bagu": "Baguette",
        "Souffy": "Souffle",
        "Quicheo": "Quiche",
        "Quicheoe": "Quiche",
        "Quicheae": "Quiche",
        "Amond": "Almond",
        "Buerr": "Beurre",
        "Cheesecak": "Cheesecake",
        "Aman": "Amann",
        "Scon": "Scone",
        "Muff": "Muffin",
        "Cak": "Cake",
        "Ro": "Roll",
        "Ana": "Banana",
        "Bi": "Banana",
        "Hwcc": "WWCC",
        "Slic": "Slice",
        "Row": "Roll",
        "Mbi": "MB",
        "Pi": "Pie",
        "Souff]e": "Souffle",
        "Cook Le": "Cookie",
        "Cook Cae": "Cookie",
        "Veggie Quicheo": "Veggie Quiche",
        "Coffee Cake Muff": "Coffee Cake Muffin",
        "Jambon Buerr": "Jambon Beurre",
        "Santa Cruz": "Santa Cruz Sandwich",
        "Kouign Aman": "Kouign Amann",
        "Olive Ciabatta Ee": "Olive Ciabatta",
        "Olive Ciabatta Bread": "Olive Ciabatta",
        "WWCC Cookie Cae": "WWCC Cookie",
        "Blueberry Co": "Blueberry Coffee Cake Muffin",
        "Seasona Polenta": "Seasonal Polenta Cake",
    }

    patterns = [
        (r'\b(?:Vegan|Pumpkin|Almond)\s+Chocola?t?e?\s+Banana\s+Muff(?:in)?', 'Vegan Chocolate Banana Muffin'),
        (r'(?:Seasonal)?\s*Polenta\s*Cake?', 'Seasonal Polenta Cake'),
        (r'\b(?:Ham & Cheese Roll?|Rol)\b', 'Ham & Cheese Roll'),
        (r'\b(?:MB|Mb|MB X|MB\'i)\b', 'MB'),
        (r'\s*\(?GF\)?', '(GF)'),
        (r'\bNultigrain\b', 'Multigrain'),
        (r'\bCo\s+(?=Cake|Coffee)\b', 'Coffee'),
        (r'Chocolate\s+(?:Bi|Ana)\s+(?:Muff|Muffin)', 'Chocolate Banana Muffin'),
        (r'(?:Mini\s+)?Mango\s+Lassi\s+Cheesecak[e]?', 'Mango Lassi Cheesecake'),
        (r'Coconut\s+Cream\s+Pi[e]?', 'Coconut Cream Pie'),
        (r'Blueberry\s+(?:Co|Coffee)\s+(?:Cake\s+)?(?:Muff|Muffin)(?:in)?', 'Blueberry Coffee Cake Muffin'),
        (r'(?:Chocolate\s+)?Almond\s+(?:Crx|Croissant)', 'Almond Croissant'),
        (r'Santa Cruz Sandwich (?:Vegan|Sandwich)', 'Santa Cruz Sandwich'),
        (r'(?:% Ww%|% WW \$)', '100% WW'),
    ]

    cleaned = item.strip()
    for old, new in standard_products.items():
        cleaned = re.sub(rf'\b{old}\b', new, cleaned, flags=re.IGNORECASE)

    for pattern, replacement in patterns:
        cleaned = re.sub(pattern, replacement, cleaned)

    words = cleaned.split()
    abbrev = {'MB', 'PAC', 'WWCC', 'GF', 'WW', 'XL'}
    words = [w.upper() if w.upper() in abbrev else w.capitalize() for w in words]
    return ' '.join(words)

def post_standardize_clean(item):
    patterns = [
        (r'\s*[-|:;,]+$', ''),  #  trailing punctuation
        (r'\s*[0-9]+$', ''),  #  trailing numbers
        (r'\s*\.+$', ''),  #  trailing dots
        (r'\s*\b(?:In|Of|On|A|I|Q)\b$', ''),  #  trailing words
        (r'\b[0-9]+\b', ''),  #  standalone numbers
        (r'\s{2,}', ' '),  # extra spaces
        (r'^\s*[&]\s*', ''),  #  leading &
        (r'(?<=\S)\s*&\s*$', ''),  #  trailing &
        (r'\s+$', ''),  #  trailing spaces
        (r'\s*(?:\'|\")\s*$', ''),  #  trailing quotes
        (r'(?<=\w)\s*\$\s*$', ''),  #  trailing dollar signs
        (r'\s+(?:f|i)$', ''),  #  trailing f or i
    ]

    cleaned = item.strip()
    for pattern, replacement in patterns:
        cleaned = re.sub(pattern, replacement, cleaned)

    if len(cleaned) <= 1 or not re.search(r'[A-Za-z]{2,}', cleaned):
        return ''

    return cleaned.strip()

def is_waste_section_header(line):
    """Check for waste section header in various formats"""
    cleaned_line = re.sub(r'[*_#]', '', line.strip())
    logging.info(f"is_waste check -- after cleaning markdown: '{cleaned_line}'")
    
    waste_patterns = [
        (r'^WASTE:?$'),
        (r'^WASTE REPORT:?$'),
        (r'^WASTED:?$'),
        (r'^WASTE ITEMS:?$')
    ]

    return any(re.match(pattern, cleaned_line, re.IGNORECASE) for pattern in waste_patterns)


def parse_text(text, email_date=None):
    lines = text.split('\n')
    data = []
    date = None

    for idx, line in enumerate(lines):
        if "Ordered:" in line:
            date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{2}\s+\d{1,2}:\d{2}\s+[APM]{2})', line)
            if date_match:
                raw_date = date_match.group(1)
                try:
                    parsed_date = datetime.datetime.strptime(raw_date, "%m/%d/%y %I:%M %p")
                    date = parsed_date.strftime("%m/%d/%Y")
                except ValueError:
                    try:
                        cleaned_date = re.sub(r'\s+', ' ', raw_date).strip()
                        parsed_date = datetime.datetime.strptime(cleaned_date, "%m/%d/%Y %I:%M %p")
                        date = parsed_date.strftime("%m/%d/%Y")
                    except ValueError:
                        date = None
        
        # if we find date, find image's waste data 
        match = re.search(r"(\d+)\s+Wasted\s+(.+)", line)
        if match and date:
            waste_count = int(match.group(1).strip())
            menu_item = match.group(2).strip()
            
            
            cleaned_menu_item = clean_menu_item(menu_item)
            stdz_menu_item = standardize_menu_item(cleaned_menu_item)
            final_menu_item = post_standardize_clean(stdz_menu_item)
            
            if len(final_menu_item) <= 1 or final_menu_item in ['', ' ']:
                continue
            
            data.append([date, final_menu_item, waste_count])
    
    # check email text if no image data found
    if not data:
        logging.info("No image data found, checking email text format")
        if not date:
            date = email_date
            logging.info(f"Using email date: {date}")
        
        if not date:
            logging.warning("No date available (neither from image nor email)")
            return []

        if date:  
            logging.info(f"Processing email text with date: {date}")
            in_waste_section = False
            waste_section_found = False  # check if waste section text of email body is reached
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                if is_waste_section_header(line):
                    logging.info(f"found waste section header in line: {line}")
                    in_waste_section = True
                    waste_section_found = True
                    continue
                
                if in_waste_section:
                    logging.info(f"processing line in waste section: {line}")
                    if ':' in line:
                        item, count = line.split(':', 1)
                        item = item.strip()
                        count = count.strip()
                        logging.info(f"split into item: '{item}' and count: '{count}'")

                        if count.isdigit():
                            waste_count = int(count)
                            logging.info(f"found valid waste count: {waste_count}")
                            cleaned_menu_item = clean_menu_item(item)
                            logging.info(f"after clean_menu_item: '{cleaned_menu_item}'")
                            stdz_menu_item = standardize_menu_item(cleaned_menu_item)
                            logging.info(f"after standardize_menu_item: '{stdz_menu_item}'")
                            final_menu_item = post_standardize_clean(stdz_menu_item)
                            logging.info(f"after post_standardize_clean: '{final_menu_item}'")

                            if len(final_menu_item) <= 1 or final_menu_item in ['', ' ']:
                                logging.info(f"skipping invalid menu item: '{final_menu_item}'")
                                continue
                            
                            data.append([date, final_menu_item, waste_count])
                            logging.info(f"added waste data: {[date, final_menu_item, waste_count]}")
                        else:
                            logging.info(f"skipping line - count isn't a digit: '{count}'")
                    else:
                        logging.info(f"skipping line - no colon found: '{line}'")

            if not waste_section_found:
                logging.warning("no waste section found in email text")
            elif not data:
                logging.warning("waste section found but no valid waste data extracted")
    
    return data
