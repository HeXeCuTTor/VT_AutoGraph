import requests
from pprint import pprint
import json


class Authograph:

    def __init__(self, HOST, username: str, password: str):
        HOST = f'{HOST}Login?UserName={username}&Password={password}&UTCOffset=000'
        self.token = requests.get(HOST).text

    def get_headers(self):
        return { 
            'AG-TOKEN': f'{self.token}'
        }
    def add_vehicles(self,vehicle_list):
        with open ('dct_enum_devices.json', 'w', encoding="utf8") as f:
            json.dump(vehicle_list, f, sort_keys=True, ensure_ascii = False, indent = 2)

    def add_TripsTotal(self,vehicle_trips):
        with open ('fct_devices_trips.json', 'w', encoding="utf8") as f:
            json.dump(vehicle_trips, f, sort_keys=True, ensure_ascii = False, indent = 2)

    def add_tracks(self,vehicle_tracks):
        with open ('fct_devices_tracks.json', 'w', encoding="utf8") as f:
            json.dump(vehicle_tracks, f, sort_keys=True, ensure_ascii = False, indent = 2)

    def download(self, HOST):
        vehicle_list = []
        vehicle_dict = {}
        vehicle_trips = []
        trips_dict = {}
        vehicle_tracks = []
        tracks_dict = {}
        headers = self.get_headers()
        current_datetime = input('Введите дату в формате YYYYMMDD без пробелов: ')
        schema = requests.get(f'{HOST}EnumSchemas', headers=headers).json()
        schema_id = schema[0].get('ID')
        source = requests.get(f"{HOST}EnumDevices?SchemaID={schema_id}", headers = headers).json()
        # pprint(source)
        item = source.get('Items')
        # pprint(item)
        for items in item:
            # pprint(items)
            Properties = (items.get('Properties'))
            # print(Properties[1])
            vehicle_dict['Serial'] = items.get('Serial')
            vehicle_dict['Allowed'] = items.get('Allowed')
            vehicle_dict['Name'] = items.get('Name')
            vehicle_dict['IsAreaEnabled'] = items.get('IsAreaEnabled')
            vehicle_dict['ID'] = items.get('ID')
            for info in Properties:
                for key,value in info.items():
                    if value == 'Alias':
                        vehicle_dict['Alias'] = info.get('Value')
                    if value == 'Branch':    
                        vehicle_dict['Branch'] = info.get('Value')
                    if value == 'Region':
                        vehicle_dict['Region'] = info.get('Value')
                    if value == 'VehicleRegNumber':
                        vehicle_dict['RegNumber'] = info.get('Value')           
            print(items.get('ID'))
            vehicle_list.append(vehicle_dict)
            # time.sleep(10)          #ПРИ ЗАПУСКЕ ВНЕ AIRFLOW ИСПОЛЬЗОВАТЬ СТРОКУ 
            vehicle_trips_all = requests.get(f"{HOST}GetTripsTotal?SchemaID={schema_id}&IDs={vehicle_dict.get('ID')}&SD={current_datetime}-0000&ED={current_datetime}-2359&tripSplitterIndex=0",
                                             headers = headers
                                            ).json()
            TripYear = current_datetime[0:4]
            TripMonth = current_datetime[0:6]
            TripDay = current_datetime[0:8]          
            trips_dict['LastCoords'] = (vehicle_trips_all.get(vehicle_dict.get('ID'))).get('_LastCoords')
            # print(LastCoords)
            trips_dict['LastData'] = (vehicle_trips_all.get(vehicle_dict.get('ID'))).get('_LastData')
            trips_dict['ProcessingTime'] = (vehicle_trips_all.get(vehicle_dict.get('ID'))).get('ProcessingTime')
            if trips_dict.get('ProcessingTime') == None:
                trips_dict['ProcessingTime'] = (vehicle_trips_all.get(vehicle_dict.get('ID'))).get('processingTime')
            trips_dict['VRN'] = (vehicle_trips_all.get(vehicle_dict.get('ID'))).get('VRN')
            trips_dict['StartDate'] = (vehicle_trips_all.get(vehicle_dict.get('ID'))).get('SD') 
            trips_dict['Enddate'] = (vehicle_trips_all.get(vehicle_dict.get('ID'))).get('ED')
            # pprint(((vehicle_trips_all.get(DeviceID_test)).get('Trips')[0]).get('Total'))
            try:
                trips_dict['Datetimefirst'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('DateTime First')
                trips_dict['Datetimelast'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('DateTime Last')
                trips_dict['longitudefirst'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('Longitude First')
                trips_dict['longitudelast'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('Longitude Last')
                trips_dict['latitudefirst'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('Latitude First')
                trips_dict['latitudelast'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('Latitude Last')
                trips_dict['Startoffirstreg'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('StartOfFirstReg')
                trips_dict['Endoflastreg'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('EndOfLastReg')
                trips_dict['Totalduration'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('TotalDuration')
                trips_dict['moveduration'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('MoveDuration')
                trips_dict['parkduration'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('ParkDuration')
                trips_dict['totaldistance'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('TotalDistance')
                trips_dict['startoffirstpark'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('StartOfFirstPark')
                trips_dict['endoflastpark'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('EndOfLastPark')
                trips_dict['startoffirstmove'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('StartOfFirstMove')
                trips_dict['endoflastmove'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('EndOfLastMove')
                trips_dict['parkcount'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('ParkCount')
                trips_dict['maxspeed'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('MaxSpeed')
                trips_dict['averagespeed'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('AverageSpeed')
                trips_dict['overspeedcount'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('OverspeedCount')
                trips_dict['firstlocation'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('FirstLocation')
                trips_dict['lastlocation'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('LastLocation')
                trips_dict['sensor1on_dur'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('Sensor1ON_dur')
                trips_dict['sensor1f_on_time'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('Sensor1F_ON_time')
                trips_dict['engine1motohours'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('Engine1Motohours')
                trips_dict['engine1mhonparks'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('Engine1MHOnParks')
                trips_dict['engine1fuelconsum'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('Engine1FuelConsum')
                trips_dict['engine1fuelconsumper100km'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('Engine1FuelConsumPer100km')
                trips_dict['dqoverspeedpointsdiff'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('DQOverspeedPoints Diff')
                trips_dict['dqexcessaccelpointsdiff'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('DQExcessAccelPoints Diff')
                trips_dict['dqexcessbrakepointsdiff'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('DQExcessBrakePoints Diff')
                trips_dict['dqemergencybrakepointsdiff'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('DQEmergencyBrakePoints Diff')
                trips_dict['dqexcessrightpointsdiff'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('DQExcessRightPoints Diff')
                trips_dict['dqexcessleftpointsdiff'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('DQExcessLeftPoints Diff')
                trips_dict['dqexcessbumppointsdiff'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('DQExcessBumpPoints Diff')
                trips_dict['dqpointsdiff'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('DQPoints Diff')
                trips_dict['dqrating'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Trips')[0]).get('Total').get('DQRating')
                trips_dict['canotherparameter2'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Total')).get('CANOtherParameter2 Last')
                trips_dict['canotherparameter3'] = ((vehicle_trips_all.get(vehicle_dict.get('ID'))).get('Total')).get('CANOtherParameter3 Last')
            except:
                pass
            trips_dict['tripyear'] = TripYear
            trips_dict['tripmonth'] = TripMonth
            trips_dict['tripday'] = TripDay
            vehicle_trips.append(trips_dict)
            trips_dict = {}
            vehicle_tracks_all = requests.get(f"{HOST}GetTrack?SchemaID={schema_id}&IDs={vehicle_dict.get('ID')}&SD={current_datetime}-0000&ED={current_datetime}-2359&tripSplitterIndex=0",
                                             headers = headers
                                            ).json()
            # pprint(vehicle_tracks_all)
            if vehicle_tracks_all.get(vehicle_dict.get('ID')) != []:
                tracks_dict['DT'] = (vehicle_tracks_all.get(vehicle_dict.get('ID'))[0]).get('DT')
                tracks_dict['Speed'] = (vehicle_tracks_all.get(vehicle_dict.get('ID'))[0]).get('Speed')
                tracks_dict['Lat'] = (vehicle_tracks_all.get(vehicle_dict.get('ID'))[0]).get('Lat')
                tracks_dict['Lng'] = (vehicle_tracks_all.get(vehicle_dict.get('ID'))[0]).get('Lng')
                tracks_dict['Photos'] = (vehicle_tracks_all.get(vehicle_dict.get('ID'))[0]).get('Photos')
                tracks_dict['TrackYear'] = TripYear
                tracks_dict['TrackMonth'] = TripMonth
                tracks_dict['TrackDay'] = TripDay
            else:
                tracks_dict['DT'] = 'NULL'
                tracks_dict['Speed'] = 'NULL'
                tracks_dict['Lat'] ='NULL'
                tracks_dict['Lng'] = 'NULL'
                tracks_dict['Photos'] = 'NULL'
                tracks_dict['TrackYear'] = 'NULL'
                tracks_dict['TrackMonth'] = 'NULL'
                tracks_dict['TrackDay'] = 'NULL'
            tracks_dict['DeviceID'] = vehicle_dict.get('ID')
            vehicle_tracks.append(tracks_dict)
            tracks_dict = {}
            vehicle_dict = {}
        self.add_vehicles(vehicle_list) 
        vehicle_list = []
        self.add_TripsTotal(vehicle_trips)
        vehicle_trips = []
        self.add_tracks(vehicle_tracks)
        vehicle_tracks = []
        source_getted = pprint("ok")
        return source_getted


if __name__ == '__main__':
    with open('text.txt', 'rt', encoding='utf=8') as file:
        words = []
        for word in file:
            words.append(word.strip()) 
    HOST = words[0]
    username = words[1]
    password = words[2]
    authograph = Authograph(HOST,username,password)
    data = Authograph.download(authograph, HOST)

