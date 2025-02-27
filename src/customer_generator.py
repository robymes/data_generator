"""
Module for generating synthetic customer data.
"""

import random
import string
import polars as pl
import numpy as np
from faker import Faker
from datetime import datetime
from tqdm import tqdm
from typing import List, Dict, Any, Optional

from config import (
    NUM_CUSTOMERS, COUNTRY_WEIGHTS, COUNTRY_FILL_RATE, DOB_FILL_RATE,
    EMAIL_FILL_RATE, PHONE_FILL_RATE, ISO_COUNTRY_CODES, NATIVE_COUNTRY_NAMES,
    EMAIL_DOMAINS, DOB_FORMATS, CUSTOMER_SOURCES, CUSTOMER_DUPLICATION_RATE,
    EMAIL_PHONE_MATCH_RATE, NAME_MATCH_WITH_TYPO_RATE
)

# Import DBManager for batch processing
from db_manager import DBManager

# Initialize faker
fake = Faker(['en_US'])  # Start with a single base locale
Faker.seed(42)  # For reproducibility
random.seed(42)
np.random.seed(42)

# Dictionary with common providers for basic name/email generation
common_locale_providers = {
    'en_US': fake,
    'en_GB': None,
    'de_DE': None,
    'fr_FR': None,
    'es_ES': None,
    'it_IT': None,
    'ja_JP': None,
    'zh_CN': None
}

# Lazy-load specific locales only when needed
def get_locale_faker(country: str):
    """Get a Faker instance for the specified country with lazy loading."""
    # Map from country to locale
    country_to_locale = {
        "China": "zh_CN",
        "India": "en_IN",
        "United States": "en_US",
        "Indonesia": "id_ID",
        "Pakistan": "en_GB",  # Use English locale for Pakistan as fallback
        "Brazil": "pt_BR",
        "Nigeria": "en_GB",  # Use English locale for Nigeria as fallback
        "Bangladesh": "en_GB",  # Use English locale for Bangladesh as fallback
        "Russia": "ru_RU",
        "Mexico": "es_MX",
        "Japan": "ja_JP",
        "Germany": "de_DE",
        "France": "fr_FR",
        "United Kingdom": "en_GB",
        "Italy": "it_IT",
        "South Korea": "ko_KR",
        "Spain": "es_ES",
        "Canada": "en_CA",
        "Saudi Arabia": "ar_SA",
        "Australia": "en_AU",
    }
    
    # Get the locale for this country, defaulting to en_US
    locale = country_to_locale.get(country, "en_US")
    
    # Check if we have a cached instance
    if locale in common_locale_providers and common_locale_providers[locale] is not None:
        return common_locale_providers[locale]
    
    # Try to load the locale, falling back to en_US if there's an issue
    try:
        # For common locales, cache them
        if locale in common_locale_providers:
            faker_instance = Faker(locale)
            common_locale_providers[locale] = faker_instance
            return faker_instance
        else:
            # For less common locales, just create a temporary instance
            return Faker(locale)
    except Exception as e:
        print(f"Warning: Error loading locale {locale}: {str(e)}. Falling back to en_US.")
        return fake  # Return the default en_US faker

# Function to generate a random 10-character unique customer ID
def generate_customer_id() -> str:
    """Generate a random 10-character alphanumeric customer ID."""
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))

# Function to add typos to names (randomly)
def add_typo(text: str, typo_probability: float = 0.2) -> str:
    """Add realistic typos to text with a given probability."""
    if random.random() > typo_probability:
        return text
    
    if not text:
        return text
    
    typo_type = random.choice(['swap', 'missing', 'double', 'adjacent'])
    
    if typo_type == 'swap' and len(text) >= 2:
        # Swap two adjacent characters
        pos = random.randint(0, len(text) - 2)
        text_list = list(text)
        text_list[pos], text_list[pos + 1] = text_list[pos + 1], text_list[pos]
        return ''.join(text_list)
    
    elif typo_type == 'missing' and len(text) >= 3:
        # Remove a character
        pos = random.randint(0, len(text) - 1)
        return text[:pos] + text[pos+1:]
    
    elif typo_type == 'double' and len(text) >= 1:
        # Double a character
        pos = random.randint(0, len(text) - 1)
        return text[:pos+1] + text[pos] + text[pos+1:]
    
    elif typo_type == 'adjacent' and len(text) >= 2:
        # Press an adjacent key on the keyboard
        pos = random.randint(0, len(text) - 1)
        char = text[pos].lower()
        
        # Simple adjacent key mapping
        adjacents = {
            'a': 'sq', 'b': 'vng', 'c': 'xvd', 'd': 'sfec', 'e': 'wrd',
            'f': 'dgt', 'g': 'fhy', 'h': 'gjy', 'i': 'uok', 'j': 'hkn',
            'k': 'jli', 'l': 'ko', 'm': 'n', 'n': 'mb', 'o': 'ipk',
            'p': 'o', 'q': 'wa', 'r': 'et', 's': 'adz', 't': 'rgy',
            'u': 'yi', 'v': 'cb', 'w': 'qes', 'x': 'czs', 'y': 'thu',
            'z': 'xs'
        }
        
        if char in adjacents and adjacents[char]:
            replacement = random.choice(adjacents[char])
            if char.isupper():
                replacement = replacement.upper()
            return text[:pos] + replacement + text[pos+1:]
    
    return text

# Function to generate phone numbers in various formats
def generate_phone_number(country: str, faker_instance=None) -> str:
    """Generate a phone number for a given country in various formats."""
    if faker_instance is None:
        faker_instance = get_locale_faker(country)
    
    # Generate a basic phone number
    try:
        phone = faker_instance.phone_number()
        
        # Apply various formatting variations
        format_type = random.randint(0, 3)
        
        if format_type == 0:
            # Standard format with international prefix
            try:
                country_code = faker_instance.country_calling_code()
                return f"{country_code} {phone}"
            except Exception:
                # Fallback if calling code not available
                return f"+{random.randint(1, 99)} {phone}"
        elif format_type == 1:
            # No spaces format
            return phone.replace(" ", "").replace("-", "").replace(".", "").replace("(", "").replace(")", "")
        elif format_type == 2:
            # With parentheses for area code
            parts = phone.split(" ")
            if len(parts) >= 2:
                return f"({parts[0]}) {' '.join(parts[1:])}"
            return phone
        else:
            # Default format
            return phone
    except Exception:
        # Fallback to basic phone format
        return f"+{random.randint(1, 99)} {random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"

# Function to generate email address based on personal info
def generate_email(name: str, surname: str, date_of_birth: Optional[str] = None, country: str = None) -> str:
    """Generate an email address, with 60% probability of using name/surname."""
    # Determine if we should use name-based email (60% probability)
    use_name_based = random.random() < 0.6
    
    # Select a domain based on weights
    # For country-specific domains, give preference to domains from that country if available
    domains = list(EMAIL_DOMAINS.keys())
    domain_weights = list(EMAIL_DOMAINS.values())
    
    # Filter out any domains starting with "example"
    valid_domains = [(domain, weight) for domain, weight in zip(domains, domain_weights) 
                     if not domain.startswith("example")]
    
    # Normalize weights after filtering
    filtered_domains, filtered_weights = zip(*valid_domains) if valid_domains else (domains, domain_weights)
    total_weight = sum(filtered_weights)
    normalized_weights = [w/total_weight for w in filtered_weights]
    
    # Select domain
    domain = random.choices(filtered_domains, weights=normalized_weights)[0]
    
    if use_name_based and name and surname:
        # Clean and normalize name/surname
        name = name.lower().replace(" ", "").replace("-", "")
        surname = surname.lower().replace(" ", "").replace("-", "")
        
        # Extract year from date_of_birth if available
        year_suffix = ""
        if date_of_birth:
            try:
                # Extract year from various date formats
                if "-" in date_of_birth:
                    year = date_of_birth.split("-")[0]
                elif "/" in date_of_birth:
                    parts = date_of_birth.split("/")
                    if len(parts[0]) == 4:  # Format YYYY/MM/DD
                        year = parts[0]
                    else:  # Format DD/MM/YYYY or MM/DD/YYYY
                        year = parts[2]
                else:
                    year = None
                
                if year and len(year) == 4:
                    # Use last 2 digits or random digits from year
                    if random.random() < 0.7:
                        year_suffix = year[-2:]
                    else:
                        year_suffix = year
            except Exception:
                year_suffix = ""
        
        # Add random digits if no year available (30% probability)
        if not year_suffix and random.random() < 0.3:
            year_suffix = str(random.randint(0, 99)).zfill(2)
        
        # Format variations
        format_type = random.randint(0, 5)
        
        if format_type == 0:
            # firstname.lastname
            username = f"{name}.{surname}"
        elif format_type == 1:
            # firstinitial+lastname
            username = f"{name[0]}{surname}"
        elif format_type == 2:
            # firstname+lastinitial
            username = f"{name}{surname[0]}"
        elif format_type == 3:
            # lastname.firstname
            username = f"{surname}.{name}"
        elif format_type == 4:
            # firstinitial+lastname+numbers
            username = f"{name[0]}{surname}{year_suffix}"
        else:
            # firstname+numbers
            username = f"{name}{year_suffix}"
        
        # Add random numbers if username is very short (less than 5 chars)
        if len(username) < 5 and not year_suffix:
            username += str(random.randint(100, 999))
        
        # Make sure the address doesn't have special characters
        username = ''.join(c for c in username if c.isalnum() or c in ['.', '_'])
        
        return f"{username}@{domain}"
    else:
        # Generate random email using Faker
        faker_instance = get_locale_faker(country)
        try:
            # Instead of looping indefinitely, try a few times then use our own domain
            for _ in range(3):  # Try max 3 times
                email = faker_instance.email()
                domain_part = email.split("@")[1]
                if not domain_part.startswith("example"):
                    return email
            
            # If we get here, we couldn't get a non-example domain
            # Use the username part but substitute our own domain
            username_part = email.split("@")[0]
            return f"{username_part}@{domain}"
                
        except Exception:
            # Fallback to basic email format
            clean_name = ''.join(c for c in name.lower() if c.isalnum()) if name else "user"
            clean_surname = ''.join(c for c in surname.lower() if c.isalnum()) if surname else str(random.randint(1000, 9999))
            return f"{clean_name}.{clean_surname}@gmail.com"

# Function to format date of birth in various formats
def format_date_of_birth(date_obj):
    """Format a date object in various formats based on weights."""
    if not date_obj:
        return None
    
    # Select format based on weights
    formats = list(DOB_FORMATS.keys())
    format_weights = list(DOB_FORMATS.values())
    selected_format = random.choices(formats, weights=format_weights)[0]
    
    # Format the date
    return date_obj.strftime(selected_format)

# Helper function to force a typo in a string
def force_typo(text: str) -> str:
    """
    Force a typo into a string, making sure the result is different from the original.
    """
    # Try up to 5 times to get a different string
    for _ in range(5):
        typo_text = add_typo(text, typo_probability=1.0)  # Force typo
        if typo_text != text:
            return typo_text
    
    # If we couldn't get a typo after 5 tries, do a simple character swap
    if len(text) >= 2:
        chars = list(text)
        idx = random.randint(0, len(chars) - 2)
        chars[idx], chars[idx + 1] = chars[idx + 1], chars[idx]
        return ''.join(chars)
    else:
        # For very short texts, just append a character
        return text + random.choice('abcdefghijklmnopqrstuvwxyz')

# Function to generate a single customer record
def generate_customer(source_id: int = None) -> Dict[str, Any]:
    """Generate a single customer record with specified characteristics."""
    # If source_id is not provided, assign based on distribution
    if source_id is None:
        source_options = list(CUSTOMER_SOURCES.keys())
        source_weights = list(CUSTOMER_SOURCES.values())
        source_id = random.choices(source_options, weights=source_weights)[0]
    
    # Select country based on weighted distribution (now based on GDP)
    countries = list(COUNTRY_WEIGHTS.keys())
    country_weights = list(COUNTRY_WEIGHTS.values())
    country_weights = np.asarray(country_weights).astype('float64') 
    country_weights /= country_weights.sum()
    country = np.random.choice(countries, p=country_weights)
    
    # Get the appropriate Faker instance for this country
    try:
        faker_instance = get_locale_faker(country)
        
        # Generate customer ID (always filled)
        customer_id = generate_customer_id()
        
        # Generate name and surname (always filled, may have typos)
        name = faker_instance.first_name()
        surname = faker_instance.last_name()
        
        # Add typos (20% probability)
        name = add_typo(name)
        surname = add_typo(surname)
        
        # Date of birth (50% filled, with varied formats)
        date_of_birth = None
        dob_obj = None
        if random.random() < DOB_FILL_RATE:
            # Generate a realistic date of birth (18-90 years old)
            try:
                dob_obj = faker_instance.date_of_birth(minimum_age=18, maximum_age=90)
                date_of_birth = format_date_of_birth(dob_obj)
            except Exception:
                # Fallback to a simple random date
                year = random.randint(1940, 2005)
                month = random.randint(1, 12)
                day = random.randint(1, 28)  # Safely within all months
                date_of_birth = f"{year}-{month:02d}-{day:02d}"
        
        # Email (80% filled, with name-based patterns)
        email = None
        if random.random() < EMAIL_FILL_RATE:
            try:
                email = generate_email(name, surname, date_of_birth, country)
            except Exception:
                # Fallback to basic email format
                clean_name = ''.join(c for c in name.lower() if c.isalnum())
                clean_surname = ''.join(c for c in surname.lower() if c.isalnum())
                email = f"{clean_name}.{clean_surname}@example.net"
        
        # Phone number (75% filled)
        phone = None
        if random.random() < PHONE_FILL_RATE:
            try:
                phone = generate_phone_number(country, faker_instance)
            except Exception:
                # Fallback to basic phone format
                phone = f"+{random.randint(1, 99)} {random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
        
        # Country formatting (95% filled)
        country_value = None
        if random.random() < COUNTRY_FILL_RATE:
            # Randomly choose format: native name, English name, or ISO code
            country_format = random.randint(0, 2)
            if country_format == 0:
                # Native name
                country_value = NATIVE_COUNTRY_NAMES.get(country, country)
            elif country_format == 1:
                # ISO code
                country_value = ISO_COUNTRY_CODES.get(country, country)
            else:
                # English name
                country_value = country
    
    except Exception as e:
        # If anything fails, use a basic fallback approach
        print(f"Warning: Error generating customer data: {str(e)}. Using fallback method.")
        
        customer_id = generate_customer_id()
        name = f"User_{random.randint(1000, 9999)}"
        surname = f"Surname_{random.randint(1000, 9999)}"
        date_of_birth = None
        email = f"{name.lower()}.{surname.lower()}@example.net"
        phone = f"+{random.randint(1, 99)}{random.randint(100000000, 999999999)}"
        country_value = country
    
    return {
        "customer_id": customer_id,
        "country": country_value,
        "name": name,
        "surname": surname,
        "date_of_birth": date_of_birth,
        "email": email,
        "mobile_phone_number": phone,
        "source_id": source_id,
        "_original_country": country  # Internal field for price adjustment, will be removed before DB insert
    }

# Function to create a duplicate customer based on match type
def create_duplicate_customer(base_customer: Dict[str, Any], match_type: str = 'email_phone') -> Dict[str, Any]:
    """
    Create a duplicate customer record based on the specified match type.
    
    Parameters:
    - base_customer: The original customer record to duplicate
    - match_type: 'email_phone' for exact email/phone match, 'name' for name/surname match with possible typos
    
    Returns:
    - A new customer record that is a duplicate of the base customer with the appropriate matching fields
    """
    try:
        # Start with a new customer ID
        duplicate = base_customer.copy()
        duplicate["customer_id"] = generate_customer_id()
        
        # For email/phone match - keep same name, email and phone
        if match_type == 'email_phone':
            # Keep exact same name, email and phone
            # Change source to the opposite channel
            duplicate["source_id"] = 1 if base_customer["source_id"] == 2 else 2
            
            # Randomly vary some other fields
            if duplicate["date_of_birth"] and random.random() < 0.3:
                # Reformat the date in a different format
                dob_parts = None
                
                # Parse date based on format
                if "-" in duplicate["date_of_birth"]:
                    parts = duplicate["date_of_birth"].split("-")
                    if len(parts[0]) == 4:  # YYYY-MM-DD
                        year, month, day = parts
                    else:  # DD-MM-YYYY
                        day, month, year = parts
                    dob_parts = (year, month, day)
                elif "/" in duplicate["date_of_birth"]:
                    parts = duplicate["date_of_birth"].split("/")
                    if len(parts[0]) == 4:  # YYYY/MM/DD
                        year, month, day = parts
                    elif len(parts[2]) == 4 and len(parts[0]) <= 2:  # DD/MM/YYYY or MM/DD/YYYY
                        if int(parts[0]) <= 12 and int(parts[1]) <= 12:
                            # Ambiguous, assume DD/MM/YYYY
                            day, month, year = parts
                        else:
                            # Must be MM/DD/YYYY or DD/MM/YYYY based on values
                            if int(parts[0]) <= 12:
                                month, day, year = parts
                            else:
                                day, month, year = parts
                    dob_parts = (year, month, day)
                
                if dob_parts:
                    try:
                        year, month, day = dob_parts
                        # Use a different format
                        formats = list(DOB_FORMATS.keys())
                        selected_format = random.choice(formats)
                        date_obj = datetime(int(year), int(month), int(day))
                        duplicate["date_of_birth"] = date_obj.strftime(selected_format)
                    except Exception:
                        # If date parsing fails, leave it unchanged
                        pass
            
            # Maybe change country format
            if duplicate["country"] and random.random() < 0.3:
                original_country = duplicate["_original_country"]
                country_format = random.randint(0, 2)
                if country_format == 0:
                    # Native name
                    duplicate["country"] = NATIVE_COUNTRY_NAMES.get(original_country, original_country)
                elif country_format == 1:
                    # ISO code
                    duplicate["country"] = ISO_COUNTRY_CODES.get(original_country, original_country)
                else:
                    # English name
                    duplicate["country"] = original_country
        
        # For name match - keep similar name/surname, change other details
        elif match_type == 'name':
            # Should we add a typo?
            add_name_typo = random.random() < NAME_MATCH_WITH_TYPO_RATE
            
            if add_name_typo:
                # Add typo to either name or surname
                if random.random() < 0.5:
                    duplicate["name"] = force_typo(duplicate["name"])
                else:
                    duplicate["surname"] = force_typo(duplicate["surname"])
            
            # Generate new email if present (80% probability)
            if duplicate["email"] and random.random() < 0.8:
                try:
                    duplicate["email"] = generate_email(
                        duplicate["name"], 
                        duplicate["surname"], 
                        duplicate["date_of_birth"], 
                        duplicate["_original_country"]
                    )
                except Exception:
                    # If email generation fails, leave it unchanged
                    pass
            
            # Generate new phone if present (80% probability)
            if duplicate["mobile_phone_number"] and random.random() < 0.8:
                try:
                    duplicate["mobile_phone_number"] = generate_phone_number(duplicate["_original_country"])
                except Exception:
                    # If phone generation fails, leave it unchanged
                    pass
            
            # Change source to the opposite channel
            duplicate["source_id"] = 1 if base_customer["source_id"] == 2 else 2
        
        return duplicate
    
    except Exception as e:
        print(f"Warning: Error creating duplicate: {str(e)}. Returning a basic duplicate.")
        # Create a basic duplicate with just a new ID and opposite source
        duplicate = base_customer.copy()
        duplicate["customer_id"] = generate_customer_id()
        duplicate["source_id"] = 1 if base_customer["source_id"] == 2 else 2
        return duplicate

# Function to generate customer dataset in batches
def generate_customer_dataset_in_batches(
    num_customers: int = NUM_CUSTOMERS, 
    batch_size: int = 10000
) -> List[Dict[str, Any]]:
    """
    Generate the customer dataset in batches and store in database.
    Returns a list of all customer IDs and their original countries for transaction generation.
    """
    # Initialize DB manager
    db_manager = DBManager()
    
    # Keep track of used customer IDs to ensure uniqueness
    used_ids = set()
    all_customer_info = []
    
    # Calculate actual number of base customers (before duplications)
    num_base_customers = int(num_customers / (1 + CUSTOMER_DUPLICATION_RATE))
    num_duplicates = num_customers - num_base_customers
    
    print(f"Generating {num_base_customers} base customers and {num_duplicates} duplicates...")
    
    # Calculate number of batches for base customers
    num_batches = max(1, (num_base_customers + batch_size - 1) // batch_size)
    
    # Generate and store base customers in batches
    all_base_customers = []
    
    for batch_idx in tqdm(range(num_batches), desc="Generating base customer data"):
        # Calculate batch size (last batch may be smaller)
        current_batch_size = min(batch_size, num_base_customers - batch_idx * batch_size)
        
        # Ensure we generate at least one customer
        if current_batch_size <= 0:
            current_batch_size = 1
        
        # Generate batch of customers
        customers_batch = []
        for _ in range(current_batch_size):
            customer = generate_customer()
            
            # Ensure customer_id is unique
            while customer["customer_id"] in used_ids:
                customer["customer_id"] = generate_customer_id()
            
            used_ids.add(customer["customer_id"])
            
            # Store the customer ID and original country for transaction generation
            all_customer_info.append({
                "customer_id": customer["customer_id"],
                "country": customer["_original_country"],
                "source_id": customer["source_id"]
            })
            
            # Store the complete customer data for generating duplicates
            all_base_customers.append(customer.copy())
            
            # Remove the internal field before DB insertion
            customer_for_db = customer.copy()
            original_country = customer_for_db.pop("_original_country", None)
            customers_batch.append(customer_for_db)
        
        # Convert batch to DataFrame and load to database
        batch_df = pl.DataFrame(customers_batch)
        db_manager.load_customers(batch_df)
    
    # Determine how many duplicates we can actually create
    num_duplicates_possible = min(num_duplicates, len(all_base_customers))
    
    if num_duplicates_possible < num_duplicates:
        print(f"Warning: Can only generate {num_duplicates_possible} duplicates instead of {num_duplicates} due to available base customers.")
    
    # Generate duplicate customers if we have any base customers and duplicates to create
    if all_base_customers and num_duplicates_possible > 0:
        print(f"Generating {num_duplicates_possible} duplicate customers...")
        
        # Determine how many duplicates for each match type
        num_email_phone_matches = int(num_duplicates_possible * EMAIL_PHONE_MATCH_RATE)
        num_name_matches = num_duplicates_possible - num_email_phone_matches
        
        # Select base customers to duplicate (ensure we don't exceed available customers)
        customers_to_duplicate = random.sample(all_base_customers, num_duplicates_possible)
        
        # Apply match types
        email_phone_customers = customers_to_duplicate[:num_email_phone_matches]
        name_match_customers = customers_to_duplicate[num_email_phone_matches:num_email_phone_matches+num_name_matches]
        
        # Create duplicate batches
        duplicate_batches = []
        duplicate_batch = []
        
        # Process email/phone matches
        for base in tqdm(email_phone_customers, desc="Creating email/phone match duplicates"):
            duplicate = create_duplicate_customer(base, 'email_phone')
            
            # Ensure customer_id is unique
            while duplicate["customer_id"] in used_ids:
                duplicate["customer_id"] = generate_customer_id()
            
            used_ids.add(duplicate["customer_id"])
            
            # Add to customer info list for transactions
            all_customer_info.append({
                "customer_id": duplicate["customer_id"],
                "country": duplicate["_original_country"],
                "source_id": duplicate["source_id"]
            })
            
            # Remove internal field
            dup_for_db = duplicate.copy()
            original_country = dup_for_db.pop("_original_country", None)
            
            # Add to batch
            duplicate_batch.append(dup_for_db)
            
            # If batch is full, process it
            if len(duplicate_batch) >= batch_size:
                duplicate_batches.append(duplicate_batch)
                duplicate_batch = []
        
        # Process name matches
        for base in tqdm(name_match_customers, desc="Creating name match duplicates"):
            duplicate = create_duplicate_customer(base, 'name')
            
            # Ensure customer_id is unique
            while duplicate["customer_id"] in used_ids:
                duplicate["customer_id"] = generate_customer_id()
            
            used_ids.add(duplicate["customer_id"])
            
            # Add to customer info list for transactions
            all_customer_info.append({
                "customer_id": duplicate["customer_id"],
                "country": duplicate["_original_country"],
                "source_id": duplicate["source_id"]
            })
            
            # Remove internal field
            dup_for_db = duplicate.copy()
            original_country = dup_for_db.pop("_original_country", None)  # noqa: F841
            
            # Add to batch
            duplicate_batch.append(dup_for_db)
            
            # If batch is full, process it
            if len(duplicate_batch) >= batch_size:
                duplicate_batches.append(duplicate_batch)
                duplicate_batch = []
        
        # Add any remaining duplicates
        if duplicate_batch:
            duplicate_batches.append(duplicate_batch)
        
        # Load all duplicate batches
        for batch_idx, batch in enumerate(duplicate_batches):
            print(f"Loading duplicate batch {batch_idx+1}/{len(duplicate_batches)} ({len(batch)} records)...")
            batch_df = pl.DataFrame(batch)
            db_manager.load_customers(batch_df)
    else:
        print("No duplicates to generate.")
    
    # Close the DB connection
    db_manager.close()
    
    print(f"Total customers generated: {len(all_customer_info)}")
    return all_customer_info

# Keep original function for testing purposes
def generate_customer_dataset(num_customers: int = NUM_CUSTOMERS) -> pl.DataFrame:
    """Generate the entire customer dataset with the specified number of records."""
    customers = []
    
    # Use a set to keep track of used customer IDs to ensure uniqueness
    used_ids = set()
    
    # Use tqdm for a progress bar
    for _ in tqdm(range(num_customers), desc="Generating customer data"):
        customer = generate_customer()
        
        # Ensure customer_id is unique
        while customer["customer_id"] in used_ids:
            customer["customer_id"] = generate_customer_id()
        
        used_ids.add(customer["customer_id"])
        # Remove the internal field before returning
        customer_for_db = customer.copy()
        original_country = customer_for_db.pop("_original_country", None)  # noqa: F841
        customers.append(customer_for_db)
    
    # Convert to Polars DataFrame
    return pl.DataFrame(customers)

if __name__ == "__main__":
    # Test the customer generator
    df = generate_customer_dataset(10)
    print(df)