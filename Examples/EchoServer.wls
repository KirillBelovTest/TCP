#!/usr/bin/env wolframscript
(* ::Package:: *)

<<KirillBelov`TCPServer`


tcp = TCPServer[]


tcp["MessageHandler"] = Function[{client, data}, Echo[data]]


listener = SocketListen[8080, tcp@#&]


While[True, Pause[0.01]]
