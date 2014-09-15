def venue_response(func):

    def venue_checker(self, venue):
        # just need the venue data, not the whole API response
        if venue.get('response'):
            venue = venue['response']['venue']

            return func(self, venue)
    return venue_checker

