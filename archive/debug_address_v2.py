
import re

def normalize_address(addr):
    if not addr: return ""
    addr = addr.upper()
    # Replace non-alphanumeric with space to ensure boundaries
    addr = re.sub(r'[^A-Z0-9 ]', ' ', addr)
    
    # Standardize directions (Word bounded)
    directions = {
        'WEST': 'W', 'EAST': 'E', 'NORTH': 'N', 'SOUTH': 'S'
    }
    for full, abbr in directions.items():
        addr = re.sub(r'\b' + full + r'\b', abbr, addr)
        
    # Remove street suffixes (Word bounded)
    suffixes = ["STREET", "ROAD", "AVENUE", "DRIVE", "COURT", "PLACE", "LANE", "CIRCLE", "BOULEVARD", 
                "ST", "RD", "AVE", "DR", "CT", "PL", "LN", "CIR", "BLVD"]
    # Sort by length just in case, though regex engine usually handles alternation order or greedy matching. 
    # Better to put longer ones first in regex OR to avoid prefix matching if not using \b, but here we use \b
    suffixes.sort(key=len, reverse=True)
    pattern = r'\b(' + '|'.join(suffixes) + r')\b'
    addr = re.sub(pattern, '', addr)

    # Remove noise (Word bounded)
    # Note: CHANDL might not be a full word if "CHANDLER" -> "CHANDL"? 
    # But usually it's "CHANDL" as a token in the raw string.
    noise = ["CHANDL", "CHANDLER", "AZ", "OC", "OCALA", "FL", "MER", "MERRILL", "IN", "GARY"]
    noise.sort(key=len, reverse=True)
    noise_pattern = r'\b(' + '|'.join(noise) + r')\b'
    addr = re.sub(noise_pattern, '', addr)
    
    # Remove spaces
    addr = re.sub(r'\s+', '', addr)
    
    return addr

tests = [
    ("3274 E Hawk Pl", "3274 EHAWK PL CHANDL"),
    ("440 Marion Oaks Ln", "440 MARIONOAKS LN OC"),
    ("1140 West 62nd", "1140 W 62 ND AVE MER"),
    ("1700 W Flamingo Dr", "1700 W FLAMINGO DR C")
]

for t1, t2 in tests:
    n1 = normalize_address(t1)
    n2 = normalize_address(t2)
    print(f"'{t1}' -> '{n1}'")
    print(f"'{t2}' -> '{n2}'")
    print(f"Match: {n1 == n2}\n")
