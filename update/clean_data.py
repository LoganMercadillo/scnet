"""Cleans the submarine cable data json data collected by scrape-cables.py.
"""
class LandingPoint:
    def __init__(self, p_id, code, name, country_id, cables=[]):
        self.id = p_id
        self.code = code
        self.name = name
        self.country_id = country_id
        self.cables = cables

    def __eq__(self, other):
        return self.code == other.code
    
    def __ne__(self, other):
        return not self.__eq__(other)
    
    def __hash__(self):
        return hash(self.code)


def parse_data(data):
    cleaned_data = {"cable": [], "country": {}, "point": {},
                    "supplier": {}, "owner": {}}
    cable_id = 0
    country_id = 0
    p_id = 0
    owner_id = 0
    supplier_id = 0

    # parse each cable's data and prep for insertion into different tables 
    for cable_name in data:
        cable_data = data[cable_name]  # dictionary of the cable's data
        
        # Extract basic cable data
        cable_code = cable_data["id"]  # string (unique)
        in_progress = cable_data["is_planned"]  # boolean (true or false)
        length = cable_data["length"]  # none or string to integer
        if length:
            length = int("".join(length.split()[0].split(",")))
        notes = cable_data["notes"]  # none or string
        rfs_year = cable_data["rfs_year"]  # none or int
        rfs_string = cable_data["rfs"]  # none or string
        url = cable_data["url"]  # none or string

        # Parse more complicated data.
        lps = cable_data["landing_points"]  # list of dicts
        owners = cable_data["owners"].split(", ")  # list of strings
        suppliers =  cable_data["suppliers"]  # none or string to list of strings
        if suppliers:
            suppliers = cable_data["suppliers"].split(", ")

        # vals for cables table
        cable_values = [cable_id, cable_name, cable_code, url,
                        length, rfs_year, rfs_string, in_progress, notes]
        cleaned_data["cable"].append(cable_values)

        # vals for landing points and countries tables
        for point in lps:
            point_code = point["id"]
            # record new LandingPoint for landingPoints table if it's unseen.
            if point_code not in cleaned_data["point"]:
                point_name = point["name"]
                country_name = point["country"]

                # record the country and assign it a new country_id if unseen.
                if country_name not in cleaned_data["country"]:
                    cleaned_data["country"][country_name] = country_id
                    p_country_id = country_id
                    country_id += 1
                else:
                    p_country_id = cleaned_data["country"][country_name]

                new_point = LandingPoint(p_id=p_id, code=point_code,
                                      name=point_name, country_id=p_country_id,
                                      cables=[cable_id])
                cleaned_data["point"][point_code] = new_point
                p_id += 1
            # if we've already seen it, add this cable to the point's seen
            else:
                cleaned_data["point"][point_code].cables.append(cable_id)


        # vals for owners table
        if owners:
            for o in owners:
                if o not in cleaned_data["owner"]:
                    cleaned_data["owner"][o] = [owner_id, [cable_id]]
                    owner_id += 1
                else:
                    cleaned_data["owner"][o][1].append(cable_id)

        # vals for suppliers table
        if suppliers:
            for s in suppliers:
                if s not in cleaned_data["supplier"]:
                    cleaned_data["supplier"][s] = [supplier_id, [cable_id]]
                    supplier_id += 1
                else:
                    cleaned_data["supplier"][s][1].append(cable_id)

        cable_id += 1

    return cleaned_data
