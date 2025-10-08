"""
Merge all webshop into one stats
and also make for every webshop unique stats
TODO: filter still on alap... sap9?
TODO: ido intervallum?  / termekek sheetben
TODO: email cimek elott a szamok
"""
from unas_helper import *

def data_dir_with_file(fname: str) -> str:
    return f"../data/{fname}"

def unas_all_webshop_stats():
    pass

def xml_to_json() -> None:
    import xmltodict

    with open("../data/yesterday.xml", "r") as f:
        xml_data = xmltodict.parse(f.read())

    json_data = json.dumps(xml_data)

    with open("../data/yesterday.json", "w") as f:
        f.write(json_data)

def get_emails_count() -> dict:
    json_data = json.load(open("../data/yesterday.json", "r", encoding="utf-8"))

    email_seen = dict()

    for item in json_data["Orders"]['Order']:
        if item['Customer']['Email'] in email_seen:
            email_seen[item['Customer']['Email']] += 1
        else:
            email_seen[item['Customer']['Email']] = 1

    return email_seen

def get_valaminev():
    """ termekek sheetben az elso tablazatban vannak a termekek es
    egy adott honapban mennyit vasaroltak belole es annak a netto ara??? """
    pass

def reflexshop_stats() -> None:
    pass

def popfanatic_stats() -> None:
    pass

def merge_stats() -> None:
    pass







def main():
    print(get_emails_count())
if __name__ == "__main__":
    main()
