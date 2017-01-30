from datetime import datetime
from datetime import timedelta
import bisect
import operator


class DataETL(object):

    """DataETL class ingests event data and implements one analytic method, TopXSimpleLTVCustomers

    """

    def __init__(self):
        """initialize class attributes

        """
        super(DataETL, self).__init__()
        self.data={}

    def ingest(self, e, D=None):
        """Given event e, update data D

        :param e: a dict describing an event
        :param D: update data D
        :return: None
        """
        if not D:
            D=self.data

        D[e['key']]={k:v for k, v in e.items() if k is not 'key'}

    def TopXSimpleLTVCustomers(self, x, D=None):
        """Return the top x customers with the highest Simple Lifetime Value from data D.

        :param x: an int describing the number of customers to output
        :param D: data D
        :return: dict of top x customer_id:lifetime_value pairs
        """
        if not D:
            D=self.data

        # filter self.data to orders
        order, order_week_list=self.make_orders(D)

        # filter self.data to site vists
        visit, visit_week_list=self.make_visits(D)

        # retrieve list of unique customer ids
        unique_customers=self.get_unique_customers(order)

        # for unique customer id retrieve orders and site visits
        customer_orders={}
        customer_visits={}
        for customer in unique_customers:
            customer_orders[customer]={k:v for k,v in order.items() if 'customer_id' in v and D[k]['customer_id']==customer}
            customer_visits[customer]={k:v for k,v in visit.items() if 'customer_id' in v and D[k]['customer_id']==customer}

        # for each customer calculate ltv
        # calculate expenditures
        order_prices={}
        expenditures={}
        for customer in customer_orders:
            orders=customer_orders[customer]
            w_l={}
            exp={}
            for week in order_week_list:
                order_keys=order_week_list[week]

                for key in order_keys:
                    if key in orders:
                        if week in w_l:
                            w_l[week].append(orders[key]['total_amount'])
                        else:
                            w_l[week]=[orders[key]['total_amount']]
                if week in w_l:
                    p=w_l[week]
                    prices=[float(i.split(" ")[0]) for i in p]
                    if len(prices)>1:
                        exp[week]=sum(prices)
                    else:
                        exp[week]=prices[0]

            order_prices[customer]=w_l
            expenditures[customer]=exp

        # calculate visits
        site_visits={}
        sv_count={}
        for customer in customer_visits:
            visits=customer_visits[customer]
            w_l={}
            sv={}
            for week in visit_week_list:
                visit_keys=visit_week_list[week]

                for key in visit_keys:
                    if key in visits:
                        if week in w_l:
                            w_l[week].append(key)
                        else:
                            w_l[week]=[key]
                if week in w_l:
                    v=w_l[week]
                    sv[week]=len(v)
                else:
                    sv[week]=0

            site_visits[customer]=w_l
            sv_count[customer]=sv

        # calculate average customer value per week
        customer_exp_per_visit={}
        avg_cust_val_per_week={}
        for customer in sv_count.items():
            exp_per_visit={}
            counts=customer[1]
            for week in counts:
                customer_expenditures=expenditures[customer[0]]
                if week in customer_expenditures:
                    exp_per_visit[week]=customer_expenditures[week]/sv_count[customer[0]][week]
                else:
                    exp_per_visit[week]=0.0
            customer_exp_per_visit[customer[0]]=exp_per_visit
            avg_cust_val_per_week[customer[0]]=sum(exp_per_visit.values())/len(exp_per_visit)

        # calculate ltv
        ltv={}
        for customer in avg_cust_val_per_week.items():
            ltv[customer]=52*avg_cust_val_per_week[customer[0]]*10

        # return top x
        top=sorted(ltv.items(), key=operator.itemgetter(1), reverse=True)[:x]
        top_x={}
        for row in top:
            cons, value = row
            id, avg_value = cons
            top_x[id]=value

        return top_x

    def make_visits(self, D):
        """Create dict of visits filtered from self.data

        :param D: data
        :return: two python dicts visit and visit_week_list
        """
        visit={k:v for k,v in D.items() if D[k]['type']=='SITE_VISIT'}

        # make weeks
        # make date list
        date_list=[]
        visit_week_list={}
        for item in visit.items():
            item[1]['date']=datetime.strptime(item[1]['event_time'][:10], "%Y-%m-%d").date()
            item[1]['isoc']=item[1]['date'].isocalendar()
            date_list.append(item[1]['date'])

        new_date_list=sorted(date_list)
        reverse_new_date_list=sorted(date_list, reverse=True)

        visit_week_list=self.make_week_list(new_date_list, reverse_new_date_list, visit_week_list)

        # assign orders to weeks
        week_list_keys=sorted(visit_week_list.keys())
        for item in visit.items():
            d=item[1]['date']
            if d in week_list_keys:
                visit_week_list[d].append(item[0])
            else:
                i=bisect.bisect_left(week_list_keys, d)
                if i:
                    visit_week_list[week_list_keys[i-1]].append(item[0])
                pass

        return visit, visit_week_list

    def make_orders(self, D):
        """Create dict of orders filtered from self.data

        :param D: data
        :return: two python dicts, order and order_week_list
        """
        order={k:v for k,v in D.items() if D[k]['type']=='ORDER'}

        # make weeks
        # make date list
        date_list=[]
        order_week_list={}
        for item in order.items():
            item[1]['date']=datetime.strptime(item[1]['event_time'][:10], "%Y-%m-%d").date()
            item[1]['isoc']=item[1]['date'].isocalendar()
            date_list.append(item[1]['date'])

        new_date_list=sorted(date_list)
        reverse_new_date_list=sorted(date_list, reverse=True)

        order_week_list=self.make_week_list(new_date_list, reverse_new_date_list, order_week_list)

        # assign orders to weeks
        week_list_keys=sorted(order_week_list.keys())
        for item in order.items():
            d=item[1]['date']
            if d in week_list_keys:
                order_week_list[d].append(item[0])
            else:
                i=bisect.bisect_left(week_list_keys, d)
                if i:
                    order_week_list[week_list_keys[i-1]].append(item[0])
                pass

        return order, order_week_list

    def make_week_list(self, new_date_list, reverse_new_date_list, week_list):
        """Create a list of weeks from a list of dates

        :param new_date_list: sorted list of dates
        :param reverse_new_date_list: reverse sorted list of dates
        :param week_list: list of weeks
        :return: python list week_list
        """
        # make week list
        if new_date_list:
            first=new_date_list[0]
            next=first+timedelta(days=7)
            last=reverse_new_date_list[0]

            week_list[first]=[]
            week_list[next]=[]
            while next<last:
                next=next+timedelta(days=7)
                week_list[next]=[]

        return week_list

    def get_unique_customers(self, D):
        """Return a list of unique customer ids

        :param D: dict of data
        :return: list of unique customer ids
        """
        return [v['customer_id'] for k,v in D.items() if 'customer_id' in v]


if __name__=='__main__':
    d=DataETL()
    events=[{"type": "CUSTOMER", "verb": "NEW", "key": "96f55c7d8f42", "event_time": "2017-01-06:12:46.384Z", "last_name": "Smith", "adr_city": "Middletown", "adr_state": "AK"},
            {"type": "SITE_VISIT", "verb": "NEW", "key": "ac05e815502f", "event_time": "2017-01-06:12:45:52.041Z", "customer_id": "96f55c7d8f42", "tags": {"some key": "some value"}},
            {"type": "IMAGE", "verb": "UPLOAD", "key": "d8ede43b1d9f", "event_time": "2017-01-06:12:47:12.344Z", "customer_id": "96f55c7d8f42", "camera_make": "Canon", "camera_model": "EOS 80D"},
            {"type": "ORDER", "verb": "NEW", "key": "68d84e5d1a43", "event_time": "2017-01-06:12:55.55.555Z", "customer_id": "96f55c7d8f42", "total_amount": "19.34 USD"},

            {"type": "SITE_VISIT", "verb": "NEW", "key": "ac05e8155031", "event_time": "2017-01-24:12:45:52.041Z", "customer_id": "96f55c7d8f42", "tags": {"some key": "some value"}},
            {"type": "ORDER", "verb": "NEW", "key": "68d84e5d1a45", "event_time": "2017-01-24:12:55.55.555Z", "customer_id": "96f55c7d8f42", "total_amount": "69.96 USD"},

            {"type": "SITE_VISIT", "verb": "NEW", "key": "ac05e8155032", "event_time": "2017-01-25:12:45:52.041Z", "customer_id": "96f55c7d8f42", "tags": {"some key": "some value"}},
            {"type": "ORDER", "verb": "NEW", "key": "68d84e5d1a46", "event_time": "2017-01-25:12:55.55.555Z", "customer_id": "96f55c7d8f42", "total_amount": "43.12 USD"},

            {"type": "SITE_VISIT", "verb": "NEW", "key": "ac05e8155033", "event_time": "2017-01-20:12:45:52.041Z", "customer_id": "96f55c7d8f42", "tags": {"some key": "some value"}},

            {"type": "SITE_VISIT", "verb": "NEW", "key": "ac05e8155034", "event_time": "2017-01-21:12:45:52.041Z", "customer_id": "96f55c7d8f42", "tags": {"some key": "some value"}},

            {"type": "SITE_VISIT", "verb": "NEW", "key": "ac05e8155035", "event_time": "2017-01-22:12:45:52.041Z", "customer_id": "96f55c7d8f42", "tags": {"some key": "some value"}},

            {"type": "CUSTOMER", "verb": "NEW", "key": "96f55c7d8f43", "event_time": "2017-01-15:12:46.384Z", "last_name": "Jones", "adr_city": "Uppertown", "adr_state": "FL"},
            {"type": "SITE_VISIT", "verb": "NEW", "key": "ac05e8155030", "event_time": "2017-01-15:12:45:52.041Z", "customer_id": "96f55c7d8f43", "tags": {"some key": "some value"}},
            {"type": "IMAGE", "verb": "UPLOAD", "key": "d8ede43b1da0", "event_time": "2017-01-15:12:47:12.344Z", "customer_id": "96f55c7d8f43", "camera_make": "Canon", "camera_model": "EOS 80D"},
            {"type": "ORDER", "verb": "NEW", "key": "68d84e5d1a44", "event_time": "2017-01-15:12:55.55.555Z", "customer_id": "96f55c7d8f43", "total_amount": "12.34 USD"}]

    for event in events:
        d.ingest(event)

    res=d.TopXSimpleLTVCustomers(1)
    print(res)