
def main(request):

    geo_json_geometries = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-121.71851098537444, 46.79135197322285],
                            [-121.71840906143188, 46.79112793274763],
                            [-121.71777606010436, 46.79102142137493],
                            [-121.71779215335847, 46.79128586301569],
                            [-121.71851098537444, 46.79135197322285]
                        ]
                    ]
                },
                "properties": {
                    "name": "Meadow1",
                    "description": "Meadow1"
                }
            }
        ]
    }
    out_dict = {'count': len(geo_json_geometries['features']),
                'feature': geo_json_geometries['features'],
                'output': ""}

    return out_dict
