lib/playback/index.ts => player definition
#running is async thread that runs on initialization. resolves when player closes
abort is public promise to shut down player
#close resolves
#abort rejects w/ error
#run
calls #runInit for video track passed into Player
calls #runInit for _the_ audio track (note: hardcoded assumption of 1 audio track)

        calls #runTrack on the A&V tracks
    #runInit
        subscribes to the track via client connection instance
        fetches init chunk from subscription
        initializes the decoder (playback worker)
    #runTrack
        subscribes to the track via #connection

lib/transport/connection.ts
manages quic (WebTranspor) connection
manages messaging (subscriptions/announcements/etc)
receives control/objects until closed

lib/playback/backend.ts
@todo
lib/playback/worker/index.ts
@todo
