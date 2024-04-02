import pydriva

def split_list(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

def gen_input():

    list = [
        'https://www.google.com',
        'https://www.yahoo.com',
        'https://www.google.com',
        'https://www.yahoo.com',
        'https://www.google.com',
        'https://www.yahoo.com',
        'https://www.google.com',
        'https://www.yahoo.com',
        'https://www.google.com',
        'https://www.yahoo.com',
        'https://www.google.com',
        'https://www.yahoo.com',
        'https://www.google.com',
        'https://www.yahoo.com',
        'https://www.google.com',
        'https://www.yahoo.com',
        'https://www.google.com',
        'https://www.yahoo.com',
        'https://www.google.com',
        'https://www.yahoo.com',
        'https://www.google.com',
        'https://www.yahoo.com',
        'https://www.google.com',
        'https://www.yahoo.com',
        'https://www.google.com',
        'https://www.yahoo.com',
        'https://www.google.com',
        'https://www.yahoo.com',
        'https://www.google.com',
        'https://www.yahoo.com'

    ]

    return split_list(list, 8)
    
def manage_output(list, i):
    print(i)
    print(list)

if __name__=='__main__':

    pydriva.fast_requests.fast_request(gen_input(), manage_output)