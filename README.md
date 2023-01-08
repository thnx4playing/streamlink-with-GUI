# Context
I was in the search for a twitch stream ripper that would monitor and save streams to my twitch synology folder to watch on plex while I'm unable to watch online.

I could not find an existing image that would respond exactly to my needs so I combined what I could find from few existing projects (see credits here below)

I decided to build it automatically on docker hub to access from my swarm nodes, when I saw many downloads, I decided to document a bit more the project, for my personnal experience and to encourage reusability

Questions, suggestions, requests, reach me out on [![alt text][1.1]][1]

I'm also interested with new projects for automation of daily popular tasks, don't hesitate, I'm waiting for new ideas

# Notes

## 2.0
major refactoring and improvements to the code, respect standards and best practices 
parse title stream and add to filename
added contribution from [zerobell-lee](https://github.com/zerobell-lee) to disable ads by authenticating as a twitch user 

## 1.9.0 - 1.9.1
Move to python 3.11
Various improvements to project, cleanup and dependencies management

## 1.8.0 - 1.8.5
Full rework with support of OAuth2 token management
Review of helix twitch operation and simplification of the flow

Upgrade python python:3.9.1-alpine3.12

## 1.7.0 - 1.7.1
Upgrade streamlink for support of twitch API recent changes

## 1.6.0
Upgrade python
Adapt recorder script to rely on upgrade version of twitch API

## 1.5.0
Upgrade python
Explicit version of python to control the release cycle of base images

## 1.4.0
Games list filter selection
Upgrade python

## 1.3.0
Introducing multi-stage building

# Credits
Thanks to the work of the original owners, so far I used and adapted python scripts from 

https://github.com/Neolysion/Twitch-Recorder/blob/master/check.py

https://www.junian.net/2017/01/how-to-record-twitch-streams.html

I cleaned up and adapted following my requirements and added a slack integration

All rights are reserved to the original script owners, tell me if I need to remove those however, I might be working on a rewrite from scratch removing few dependencies to python libraries

# Quality
Quality is specified within the stream, any twitch quality specified existing for the stream can be defined

Keywords can always be used
* best
* worst

# Variables
## timer
Fill in twitch clientid to interact with twitch API and retrieve status of stream from stream list

    timer=360


## clientid
Fill in twitch clientid to interact with twitch API and retrieve status of stream from stream list

    clientid=xxxxxxxx

## clientsecret
Fill in twitch clientsecret to interact with twitch API and retrieve status of stream from stream list

    clientsecret=xxxxxxxx

## twitchaccountauth
Fill in twitch account token(it is different from client token)

This is for disabling embedded ads, if you're a subscriber of the target streamer.

You can find how to get this token on [Streamlink Documentation](https://streamlink.github.io/cli/plugins/twitch.html).

    twitchaccountauth=xxxxxxxxxx

## slackid
Fill in slack if you want recod start/stop notification

    slackid=xxxxxxxxx

# Docker
    docker run -d --rm \
    -v twitch:/download \
    -e timer=360 \
    -e user=heromarine \
    -e quality=best \
    -e clientid=XxX \
    -e clientsecret=XxX \
    -e slackid=XxX \
    liofal/streamlink:latest

# Compose

## Startup
To run a test service

    ./docker-compose -f dockerimages/streamlink/docker-compose.yml up -d test

## clientid.env
Specify the clientid.env file using the clientid.env.example delivered

## default.env
you can specify the default for compose here

# Volume
    /download 

_**Warning:** The folder does not exist in the container and need te be created as a volume in order to be accessed from outside your container, you should map it if you want to access it_


[1.1]: http://i.imgur.com/tXSoThF.png (twitter icon with padding)
[1]: http://www.twitter.com/liofal
