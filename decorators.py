def venue_response(func):

    def venue_checker(*args):
        # just need the venue data, not the whole API response
        for i, arg in enumerate(args):
            if type(arg) == type(dict):
                if arg.get('response'):
                    args[i] = arg['response']['venue']

            return func(*args)
    return venue_checker

