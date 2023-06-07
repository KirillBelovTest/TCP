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
(*Requarements*)


Once[If[PacletFind["KirillBelov/Internal"] === {}, PacletInstall["KirillBelov/Internal"]]]; 
Once[If[PacletFind["KirillBelov/Objects"] === {}, PacletInstall["KirillBelov/Objects"]]]; 


(* ::Section::Closed:: *)
(*Begin package*)


BeginPackage["KirillBelov`TCPServer`", {"KirillBelov`Objects`", "KirillBelov`Internal`"}]; 


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
	client = packet["SourceSocket"]; (*SocketObject[]*)
	extendedPacket = getExtendedPacket[server, client, packet]; (*Association[]*)
	logger[StringTemplate["Received `DataLength` bytes of message with length `ExpectedLength` bytes."][extendedPacket]]; 

	If[extendedPacket["Completed"], 
		message = getMessage[server, client, extendedPacket]; (*ByteArray[]*)
		result = invokeHandler[server, client, message]; (*ByteArray[] | Null*)
		sendResponse[server, client, result]; 
		logger[StringTemplate["Sending `` bytes."][Length[result]]]; 
		clearBuffler[server, client], 
	(*Else*)
		logger[StringTemplate["Buffered `StoredLength` bytes of `ExpectedLength`. Adding `DataLength` bytes."][extendedPacket]]; 
		savePacketToBuffer[server, client, extendedPacket]
	]; 
]; 


(* ::Section::Closed:: *)
(*Internal methods*)


TCPServer /: getExtendedPacket[server_TCPServer, client: SocketObject[uuid_String], packet_Association] := 
Module[{data, dataLength, buffer, last, expectedLength, storedLength, completed, logger}, 
	
	logger = server["Logger"]; 
	data = packet["DataByteArray"]; 
	dataLength = Length[data]; 

	If[KeyExistsQ[server["Buffer"], uuid] && server["Buffer", uuid]["Length"] > 0, 
		buffer = server["Buffer", uuid]; 
		last = buffer["Part", -1]; 
		expectedLength = last["ExpectedLength"]; 
		storedLength = last["StoredLength"]; , 
	(*Else*)
		expectedLength = ConditionApply[server["CompleteHandler"], Function[Length[#2]]][client, data]; 
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
ConditionApply[server["MessageHandler"], Function[Close[#1]]][client, message]


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
(*End private context*)


End[]; 


(* ::Section::Closed:: *)
(*End package*)


EndPackage[]; 
