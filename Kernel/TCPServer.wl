(* ::Package:: *)

(* ::Chapter:: *)
(*TCP Server*)


(*+-------------------------------------------------+
  |                HANDLE PACKET                    |
  |                                                 |
  |              (receive packet)                   |
  |                      |                          |
  |            [get extended packet]                |
  |                      |                          |
  |                <is complete>                    |
  |           yes /             \ no                |
  |    [get message]      [save packet to buffer]   |
  |          |                   /                  |
  |   [invoke handler]          /                   |
  |          |                 /                    |
  |   [send response]         /                     |
  |          |               /                      |
  |    [clear buffer]       /                       |
  |                 \      /                        |
  |                  {next}                         |
  +-------------------------------------------------+*)


(* ::Section::Closed:: *)
(*Begin package*)


Once[If[PacletFind["KirillBelov/Objects"] === {}, PacletInstall["KirillBelov/Objects"]]]; 


BeginPackage["KirillBelov`TCPServer`", {"KirillBelov`Objects`"}]; 


(* ::Section::Closed:: *)
(*Names*)


TCPServer::usage = 
"TCPServer[opts] TCP server"; 


(* ::Section::Closed:: *)
(*Private context*)


Begin["`Private`"]; 


(* ::Section::Closed:: *)
(*Server*)


(* ::Section::Closed:: *)
(*Cosntructor*)


CreateType[TCPServer, {
	"Logger", 
	"Buffer" -> <||>, 
	"CompleteHandler" -> <||>, 
	"MessageHandler" -> <||>
}]; 


(* ::Section::Closed:: *)
(*Entrypoint*)


server_TCPServer[packet_Association] := 
Module[{logger, client, extendedPacket, message, result}, 
	logger = server["Logger"]; 
	logger["Packet received", packet]; 
	
	client = packet["SourceSocket"]; (*SocketObject[]*)
	logger["Current clinet", client]; 

	extendedPacket = getExtendedPacket[server, client, packet]; (*Association[]*)
	logger["Extended packet", extendedPacket]; 
	
	If[extendedPacket["Completed"], 
		logger["Packet completed", extendedPacket]; 
		message = getMessage[server, client, extendedPacket]; (*ByteArray[]*)
		logger["Received message", message]; 
		result = invokeHandler[server, client, message]; (*ByteArray[] | Null*)
		logger["Handler result", result]; 
		sendResponse[server, client, result]; 
		clearBuffler[server, client], 
	(*Else*)
		logger["Packet buffered", extendedPacket]; 
		savePacketToBuffer[server, client, extendedPacket]
	]; 
]; 


(* ::Section::Closed:: *)
(*Internal methods*)


TCPServer /: getExtendedPacket[server_TCPServer, client: SocketObject[uuid_String], packet_Association] := 
Module[{data, dataLength, buffer, last, expectedLength, storedLength, completed}, 
	
	data = packet["DataByteArray"]; 
	dataLength = Length[data]; 

	If[KeyExistsQ[server["Buffer"], uuid] && server["Buffer", uuid]["Length"] > 0, 
		buffer = server["Buffer", uuid]; 
		last = buffer["Part", -1]; 
		expectedLength = last["ExpectedLength"]; 
		storedLength = last["StoredLength"]; , 
	(*Else*)
		expectedLength = conditionApply[server["CompleteHandler"], Function[Length[#2]]][client, data]; 
		storedLength = 0; 
	]; 

	completed = storedLength + dataLength >= expectedLength; 

	(*Return: _Association*)
	Join[packet, <|
		"Completed" -> completed, 
		"ExpectedLength" -> expectedLength, 
		"StoredLength" -> storedLength + dataLength, 
		"DataLength" -> dataLength
	|>]
]; 


TCPServer /: getMessage[server_TCPServer, client: SocketObject[uuid_String], extendedPacket_Association] := 
If[KeyExistsQ[server["Buffer"], uuid] && server["Buffer", uuid]["Length"] > 0, 
	(*Return: _ByteArray*)
	Apply[Join] @ 
	Append[extendedPacket["DataByteArray"]] @ 
	server["Buffer", uuid]["Elements"][[All, "DataByteArray"]], 
(*Else*)
	(*Return: _ByteArray*)
	extendedPacket["DataByteArray"]
]; 


TCPServer /: invokeHandler[server_TCPServer, client_SocketObject, message_ByteArray] := 
conditionApply[server["MessageHandler"], Function[Close[#1]]][client, message]


TCPServer /: sendResponse[server_TCPServer, client_SocketObject, result: _String | _ByteArray | Null] := 
Module[{t = AbsoluteTime[]}, 
	Switch[result, 
		_String, WriteString[client, result], 
		_ByteArray, BinaryWrite[client, result], 
		Null, Null
	]
]; 


TCPServer /: savePacketToBuffer[server_TCPServer, SocketObject[uuid_String], extendedPacket_Association] := 
If[KeyExistsQ[server["Buffer"], uuid], 
	server["Buffer", uuid]["Append", extendedPacket], 
	server["Buffer", uuid] = CreateDataStructure["DynamicArray", {extendedPacket}]
]; 


TCPServer /: clearBuffer[server_TCPServer, SocketObject[uuid_String]] := 
If[KeyExistsQ[server["Buffer"], uuid], 
	server["Buffer", uuid]["DropAll"]
]; 


(* ::Section::Closed:: *)
(*Internal funcs*)


conditionApply[conditionsAndFunctions_Association: <||>, defalut_: Function[Null], ___] := 
Function[Last[SelectFirst[conditionsAndFunctions, Function[cf, First[cf][##]], {defalut}]][##]]; 


(* ::Section::Closed:: *)
(*End private context*)


End[]; 


(* ::Section::Closed:: *)
(*End package*)


EndPackage[]; 
