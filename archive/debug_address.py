
import re

def normalize_address(addr):
    if not addr: return ""
    addr = addr.upper()
    # Remove common street suffixes and directions for comparison
    suffixes = ["STREET", "ROAD", "AVENUE", "DRIVE", "COURT", "PLACE", "LANE", "CIRCLE", "BOULEVARD", "WEST", "EAST", "NORTH", "SOUTH", 
                "ST", "RD", "AVE", "DR", "CT", "PL", "LN", "CIR", "BLVD", "W", "E", "N", "S"]
    # Sort by length descending to avoid partial replacements (e.g. STREET before ST)
    suffixes.sort(key=len, reverse=True)
    
    # Remove non-alphanumeric except spaces for initial clean
    addr = re.sub(r'[^A-Z0-9 ]', '', addr)
    
    # Remove all spaces to handle merged words (e.g., MARIONOAKS vs MARION OAKS)
    addr = re.sub(r'\s+', '', addr)
    
    # Remove suffixes (some might be merged, so use substring replacement)
    for word in suffixes:
        addr = addr.replace(word, "")
        
    # Remove known city/state noise often found at the end of statement addresses
    noise = ["CHANDL", "CHANDLER", "AZ", "OC", "OCALA", "FL", "MER", "MERRILL", "IN", "GARY"]
    for word in noise:
        if addr.endswith(word):
            addr = addr[:-len(word)]
            
    return addr

addr1 = "3274 E Hawk Pl"
addr2 = "3274 EHAWK PL CHANDL"

norm1 = normalize_address(addr1)
norm2 = normalize_address(addr2)

print(f"Original 1: {addr1}")
print(f"Normalized 1: {norm1}")
print(f"Original 2: {addr2}")
print(f"Normalized 2: {norm2}")
print(f"Match: {norm1 == norm2}")
