#!flask/bin/python
from flask import Flask
from flask import request, jsonify
from flask_restplus import Resource
import requests
from google.cloud import bigquery
from google.oauth2 import service_account
from google.auth.transport import requests
import json
import csv
from werkzeug.wrappers import Response
from datetime import datetime
from io import StringIO
import datetime

server = Flask(__name__)

credentials = service_account.Credentials.from_service_account_file(
    r'flask_app/slwidgets-eedaccf3f41b.json')
project_id = 'slwidgets'
client = bigquery.Client(credentials= credentials,project=project_id)

@server.route('/')
def index():
    return "Hello, World!"

@server.route('/big-query/order-convertion-rate/<int:days>', methods=['GET'])
def GetConvertionRate(days):
    print('days ', days)
    sql = """
                    SELECT 
                    SUM( totals.transactions ) as total_transactions,
                    SUM( totals.visits )  as total_visits
                    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
                    WHERE
                    _TABLE_SUFFIX BETWEEN 
                    FORMAT_DATE("%Y%m%d", DATE_SUB(DATE('2017-08-01'), INTERVAL {} DAY)) AND
                    FORMAT_DATE("%Y%m%d", DATE_SUB(DATE('2017-08-01'), INTERVAL 1 DAY));
    """.format(days)
    df = client.query(sql).to_dataframe()
    data = df.to_json()
    data = json.loads(data)
    total_transactions = data['total_transactions']['0']
    total_visits = data['total_visits']['0']

    print('---------------- ',data['total_transactions']['0'])

    response = jsonify({
        'status': 'success',
        'convertion_rate': float(total_transactions)/float(total_visits)
        })
    response.status_code = 201
    return response

@server.route('/big-query/order-convertion-rate-previous-period/<int:days>', methods=['GET'])
def GetComparisonConvertionRate(days):
    print('days ', days)
    """List of query browser"""
    print('List all query details')
    sql = """
                SELECT 
                SUM( totals.transactions ) as total_transactions,
                SUM( totals.visits )  as total_visits
                FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
                WHERE
                _TABLE_SUFFIX BETWEEN 
                FORMAT_DATE("%Y%m%d", DATE_SUB(DATE('2017-08-01'), INTERVAL {} DAY)) AND
                FORMAT_DATE("%Y%m%d", DATE_SUB(DATE('2017-08-01'), INTERVAL 1 DAY));
    """.format(days)
    df = client.query(sql).to_dataframe()
    data = df.to_json()
    data = json.loads(data)
    total_transactions = data['total_transactions']['0']
    total_visits = data['total_visits']['0']

    days2 = int(days)*2
    sql2 = """
        SELECT 
        SUM( totals.transactions ) as total_transactions,
        SUM( totals.visits )  as total_visits
        FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
        WHERE
        _TABLE_SUFFIX BETWEEN 
        FORMAT_DATE("%Y%m%d", DATE_SUB(DATE('2017-08-01'), INTERVAL {} DAY)) AND
        FORMAT_DATE("%Y%m%d", DATE_SUB(DATE('2017-08-01'), INTERVAL {} DAY));
    """.format(days2, days)

    df2 = client.query(sql2).to_dataframe()
    data2 = df2.to_json()
    data2 = json.loads(data2)
    total_transactions2 = data2['total_transactions']['0']
    total_visits2 = data2['total_visits']['0']

    print('---------------- ',data['total_transactions']['0'])

    response = jsonify({
        'status': 'success',
        'convertion_rate': float(total_transactions)/float(total_visits),
        'convertion_rate_period_before': float(total_transactions2)/float(total_visits2)
        })
    response.status_code = 201
    return response

@server.route('/big-query/order-convertion-rate/<int:days>/csv', methods=['GET'])
def GetConvertionRateCSV(days):
    print('days ', days)
    """List of query browser"""
    print('List all query details')

    sql = """
                SELECT 
                SUM( totals.transactions ) as total_transactions,
                SUM( totals.visits )  as total_visits
                FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
                WHERE
                _TABLE_SUFFIX BETWEEN 
                FORMAT_DATE("%Y%m%d", DATE_SUB(DATE('2017-08-01'), INTERVAL {} DAY)) AND
                FORMAT_DATE("%Y%m%d", DATE_SUB(DATE('2017-08-01'), INTERVAL 1 DAY));
    """.format(days)
    df = client.query(sql).to_dataframe()
    data = df.to_json()
    data = json.loads(data)
    total_transactions = data['total_transactions']['0']
    total_visits = data['total_visits']['0']
    convertion_rate = float(total_transactions)/float(total_visits)
    
    log = [
        ('1', convertion_rate)
    ]

    response = Response(generate(log), mimetype='text/csv')
    # add a filename
    response.headers.set("Content-Disposition", "attachment", filename="convertion_rate.csv")
    return response

@server.route('/big-query/get-user-profile/<int:id>', methods=['GET'])
def GetUserDetails(id):
    print('profile id ', id)
    """List of query browser"""
    print('List all query details')
    # 2248281639583218707
    sql = """
                SELECT *
                FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
                WHERE fullVisitorId = "{}";
    """.format(id)
    df = client.query(sql).to_dataframe()
    data = df.to_json()
    data = json.loads(data)
    try:
        if data:


            product_list = data['hits']['0'][0]['product']
            product_list_arr = []
            for product in product_list:
                product_list_arr.append({
                    "productSku": product['productSKU'],
                    "productName": product['v2ProductName'],
                    "itemRevenue": product['productRevenue'],
                    "productQuantity": product['productQuantity']
                })

            usedCoupon = 0
            if data['hits']['0'][0]['transaction']['transactionCoupon'] is None:
                usedCoupon = 0
            else:
                usedCoupon = 1

            hasPurchase = 0
            if data['totals']['0']['transactions'] is None:
                hasPurchase = 0
            else:
                hasPurchase = 1

            response = jsonify({
                'status': 'success',
                'data' : {"pseudoUserId": str(id),
                            "deviceCategory": data['device']['0']['deviceCategory'],
                            "platform": data['device']['0']['operatingSystem'],
                            "dataSource": data['hits']['0'][0]['page']['pagePath'],
                            "hasPurchase": hasPurchase,
                            "usedCoupon": usedCoupon,
                            "userRevenue": data['totals']['0']['totalTransactionRevenue'],
                            "acquisitionChannelGroup": data['channelGrouping']['0'],
                            "acquisitionSource": data['trafficSource']['0']['source'],
                            "acquisitionMedium": data['trafficSource']['0']['medium'],
                            "acquisitionCampaign": data['trafficSource']['0']['campaign'],
                            "userType": data['channelGrouping']['0'],
                            "firstSeen": data['hits']['0'][0]['page']['hostname'],
                            "lastSeen": data['hits']['0'][0]['page']['pagePath'],
                            "numberVisits": data['totals']['0']['visits'],
                            "numberPurchases": data['totals']['0']['transactions'],
                            "purchaseActivities": [{
                                        "activityTime": 'string, // Timestamp of the activity.',
                                        "channelGrouping": data['channelGrouping']['0'],
                                        "source": data['trafficSource']['0']['source'],
                                        "medium": data['trafficSource']['0']['medium'],
                                        "campaign": data['trafficSource']['0']['campaign'],
                                        "landingPagePath": data['hits']['0'][0]['appInfo']['landingScreenName'],
                                        "ecommerce": {
                                                        "transaction": {
                                                                "transactionId": data['hits']['0'][0]['transaction']['transactionId'],
                                                                "transactionRevenue": data['hits']['0'][0]['transaction']['transactionRevenue'],
                                                                "transactionCoupon": data['hits']['0'][0]['transaction']['transactionCoupon'],
                                                        },
                                                        "products": product_list_arr
                                                    },
                                        }],
                            "sessionId": data['visitId']['0'],
                            "sessionDate": datetime.datetime.fromtimestamp(int(data['visitStartTime']['0']) / 1e3),
                        }
                })
            response.status_code = 201
            return response
        else:
            response = jsonify({
                'status': 'Fail',
                'message': 'User ID is wrong'
                })
            response.status_code = 201
            return response
    except:
        response = jsonify({
            'status': 'Fail',
            'message': 'User ID is wrong'
            })
        response.status_code = 201
        return response


@server.route('/big-query/order-convertion-rate-group-wise/<int:days>', methods=['GET'])
def GetConvertionRateGroupBy(days):
    print('days ', days)
    sql = """
                    SELECT 
                    SUM( totals.transactions ) as total_transactions,
                    SUM( totals.visits )  as total_visits,
                    device.deviceCategory
                    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
                    WHERE
                    _TABLE_SUFFIX BETWEEN 
                    FORMAT_DATE("%Y%m%d", DATE_SUB(DATE('2017-08-01'), INTERVAL {} DAY)) AND
                    FORMAT_DATE("%Y%m%d", DATE_SUB(DATE('2017-08-01'), INTERVAL 1 DAY))
                    GROUP BY device.deviceCategory;
    """.format(days)
    df = client.query(sql).to_dataframe()
    data = df.to_json()
    data = json.loads(data)
    print(data)

    try:
        mobile_convertion_rate = float(data['total_transactions']['0'])/float( data['total_visits']['0'])
    except:
        mobile_convertion_rate = 0
    try:
        desktop_convertion_rate = float(data['total_transactions']['1'])/float( data['total_visits']['1'])
    except:
        desktop_convertion_rate = 0
    try:
        tablet_convertion_rate = float(data['total_transactions']['2'])/float( data['total_visits']['2'])
    except:
        tablet_convertion_rate = 0


    response = jsonify({
        'status': 'success',
        'mobile_convertion_rate': mobile_convertion_rate,
        'desktop_convertion_rate': desktop_convertion_rate,
        'tablet_convertion_rate': tablet_convertion_rate
        })
    response.status_code = 201
    return response


@server.route('/big-query/order-convertion-rate-two-dimention/<int:days>', methods=['GET'])
def GetConvertionRateGroupByDimention(days):
    print('days ', days)
    sql = """
            SELECT 
            SUM( totals.transactions ) as total_transactions,
            SUM( totals.visits )  as total_visits,
            device.deviceCategory as device_category,
            channelGrouping as user_group,
            (SUM( totals.transactions )/SUM( totals.visits )) as convertion_rate
            FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
            WHERE
            _TABLE_SUFFIX BETWEEN 
            FORMAT_DATE("%Y%m%d", DATE_SUB(DATE('2017-08-01'), INTERVAL {} DAY)) AND
            FORMAT_DATE("%Y%m%d", DATE_SUB(DATE('2017-08-01'), INTERVAL 1 DAY))
            GROUP BY device.deviceCategory, channelGrouping;
    """.format(days)
    df = client.query(sql).to_dataframe()
    data = df.to_json()
    data = json.loads(data)
    print(data)
    print(data['device_category'])

    response = jsonify({
        'status': 'success',
        'group 1': str(data['convertion_rate']['0']) + ' | '+str(data['device_category']['0']) + ' | '+str(data['user_group']['0']),
        'group 2': str(data['convertion_rate']['1']) + ' | '+str(data['device_category']['1']) + ' | '+str(data['user_group']['1']),
        'group 3': str(data['convertion_rate']['2']) + ' | '+str(data['device_category']['2']) + ' | '+str(data['user_group']['2']),
        'group 4': str(data['convertion_rate']['3']) + ' | '+str(data['device_category']['3']) + ' | '+str(data['user_group']['3']),
        'group 5': str(data['convertion_rate']['4']) + ' | '+str(data['device_category']['4']) + ' | '+str(data['user_group']['4']),
        'group 6': str(data['convertion_rate']['5']) + ' | '+str(data['device_category']['5']) + ' | '+str(data['user_group']['5']),
        'group 7': str(data['convertion_rate']['6']) + ' | '+str(data['device_category']['6']) + ' | '+str(data['user_group']['6']),
        'group 8': str(data['convertion_rate']['7']) + ' | '+str(data['device_category']['7']) + ' | '+str(data['user_group']['7']),
        'group 9': str(data['convertion_rate']['8']) + ' | '+str(data['device_category']['8']) + ' | '+str(data['user_group']['8']),
        'group 10': str(data['convertion_rate']['9']) + ' | '+str(data['device_category']['9']) + ' | '+str(data['user_group']['9']),
        'group 11': str(data['convertion_rate']['10']) + ' | '+str(data['device_category']['10']) + ' | '+str(data['user_group']['10']),
        'group 12': str(data['convertion_rate']['11']) + ' | '+str(data['device_category']['11']) + ' | '+str(data['user_group']['11']),
        'group 13': str(data['convertion_rate']['12']) + ' | '+str(data['device_category']['12']) + ' | '+str(data['user_group']['12']),
        'group 14': str(data['convertion_rate']['13']) + ' | '+str(data['device_category']['13']) + ' | '+str(data['user_group']['13']),
        'group 15': str(data['convertion_rate']['14']) + ' | '+str(data['device_category']['14']) + ' | '+str(data['user_group']['14']),
        'group 16': str(data['convertion_rate']['15']) + ' | '+str(data['device_category']['15']) + ' | '+str(data['user_group']['15']),
        'group 17': str(data['convertion_rate']['16']) + ' | '+str(data['device_category']['16']) + ' | '+str(data['user_group']['16']),
        'group 18': str(data['convertion_rate']['17']) + ' | '+str(data['device_category']['17']) + ' | '+str(data['user_group']['17']),
        'group 19': str(data['convertion_rate']['18']) + ' | '+str(data['device_category']['18']) + ' | '+str(data['user_group']['18']),
        'group 20': str(data['convertion_rate']['19']) + ' | '+str(data['device_category']['19']) + ' | '+str(data['user_group']['19'])
        })
    response.status_code = 201
    return response

@server.route('/big-query/order-convertion-rate-two-new/<int:days>', methods=['GET'])
def GetConvertionRateGroupByDimentionNew(days):
    print('days ', days)
    sql = """
            SELECT 
            SUM( totals.transactions ) as total_transactions,
            SUM( totals.visits )  as total_visits,
            device.deviceCategory as device_category,
            IF(totals.visits IS NOT NULL, "Returning User", "New Visitor") as user_type,
            (SUM( totals.transactions )/SUM( totals.visits )) as convertion_rate
            FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
            WHERE
            _TABLE_SUFFIX BETWEEN 
            FORMAT_DATE("%Y%m%d", DATE_SUB(DATE('2017-08-01'), INTERVAL {} DAY)) AND
            FORMAT_DATE("%Y%m%d", DATE_SUB(DATE('2017-08-01'), INTERVAL 1 DAY))
            GROUP BY device.deviceCategory, user_type;
    """.format(days)
    df = client.query(sql).to_dataframe()
    data = df.to_json()
    data = json.loads(data)
    print(data)
    response = jsonify({
        'status': 'success',
        'data': data
    })
    response.status_code = 201
    return response


def generate(log):
    data = StringIO()
    w = csv.writer(data)

    # write header
    w.writerow(('No', 'Convertion_Rate'))
    yield data.getvalue()
    data.seek(0)
    data.truncate(0)

    # write each log item
    for item in log:
        w.writerow((
            item[0],
            item[1]  # format datetime as string
        ))
        yield data.getvalue()
        data.seek(0)
        data.truncate(0)