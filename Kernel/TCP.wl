(* ::Package:: *)

(* ::Chapter:: *)
(*TCP Server*)


(* ::Program:: *)
(*+-------------------------------------------------+*)
(*|                HANDLE PACKET                    |*)
(*|                                                 |*)
(*|              (receive packet)                   |*)
(*|                      |                          |*)
(*|            [get extended packet]                |*)
(*|                      |                          |*)
(*|                <is complete>                    |*)
(*|           yes /             \ no                |*)
(*|    [get message]      [save packet to buffer]   |*)
(*|          |                   /                  |*)
(*|   [invoke handler]          /                   |*)
(*|          |                 /                    |*)
(*|   [send response]         /                     |*)
(*|          |               /                      |*)
(*|    [clear buffer]       /                       |*)
(*|                 \      /                        |*)
(*|                  {next}                         |*)
(*+-------------------------------------------------+*)


(* ::Section::Closed:: *)
(*Requarements*)


Once[Map[If[Length[PacletFind[#]] === 0, PacletInstall[#]]&][{
	"KirillBelov/Objects", 
	"KirillBelov/Internal"
}]]; 


(* ::Section::Closed:: *)
(*Begin package*)


BeginPackage["KirillBelov`TCP`", {
	"KirillBelov`Objects`", 
	"KirillBelov`Internal`"
}]; 


(* ::Section::Closed:: *)


TCPServer::usage = 
"TCPServer[opts] TCP server"; 


(* ::Section::Closed:: *)
(*Private context*)


Begin["`Private`"]; 


(* ::Section:: *)
(*Server*)


(* ::Section::Closed:: *)
(*Cosntructor*)


CreateType[TCPServer, {
	"Logger", 
	"Buffer" -> <||>, 
	"CompleteHandler" -> <||>, 
	"DefaultCompleteHandler" -> Function[Length[#DataByteArray]], 
	"MessageHandler" -> <||>, 
	"DefaultMessageHandler" -> Function[#]
}]; 


(* ::Section::Closed:: *)
(*Entrypoint*)


server_TCPServer[packet_Association] := 
Module[{logger, extendedPacket, result, extraPacket, extraPacketDataLength}, 
	extendedPacket = getExtendedPacket[server, packet]; (*Association[]*)
	
	If[extendedPacket["Completed"] && extendedPacket["Event"] === "Received", 
		With[{message = getMessage[server, extendedPacket]}, 
			extendedPacket["DataByteArray"] := message; (*ByteArray[]*)
			extendedPacket["Data"] := ByteArrayToString[message];
			extendedPacket["DataBytes"] := Normal[message];
		]; 
		result = invokeHandler[server, extendedPacket]; (*ByteArray[] | _String | Null*)
		sendResponse[server, packet, result]; 

		If[extendedPacket["StoredLength"] > extendedPacket["ExpectedLength"], 
			extraPacket = packet; 
			extraPacketDataLength = extendedPacket["StoredLength"] - extendedPacket["ExpectedLength"]; 
			extraPacket["DataByteArray"] = packet["DataByteArray"][[-extraPacketDataLength ;; ]]; 
			clearBuffer[server, packet]; 
			server[extraPacket], 
		(*Else*)
			clearBuffer[server, extendedPacket]
		]; 
		
		Return[result], 
	(*Else*)
		savePacketToBuffer[server, extendedPacket]
	]; 
]; 


(* ::Section:: *)
(*Internal methods*)


TCPServer /: getExtendedPacket[server_TCPServer, packet_Association] := 
With[{uuid = packet["SourceSocket"][[1]]}, 
	Module[{dataLength, buffer, last, expectedLength, storedLength, completed, completeHandler, defaultCompleteHandler, extendedPacket}, 
		dataLength = Length[packet["DataByteArray"]]; 

		If[KeyExistsQ[server["Buffer"], uuid] && server["Buffer", uuid]["Length"] > 0, 			
			buffer = server["Buffer", uuid]; (*DataStructure[DynamicArray]*)
			last = buffer["Part", -1]; (*Association[]*) 
			expectedLength = last["ExpectedLength"]; 
			storedLength = last["StoredLength"];, 

		(*Else*)
			completeHandler = server["CompleteHandler"]; (*Association[] | Function[]*)
			defaultCompleteHandler = server["DefaultCompleteHandler"]; (*Function[]*)
			expectedLength = ConditionApply[completeHandler, defaultCompleteHandler][packet]; 
			storedLength = 0; 
		]; 

		completed = storedLength + dataLength >= expectedLength; 

		(*Return: Association[]*)
		Join[packet, <|
			"Completed" -> completed, 
			"ExpectedLength" -> expectedLength, 
			"StoredLength" -> storedLength + dataLength, 
			"DataLength" -> dataLength
		|>]
	]
]; 


TCPServer /: getMessage[server_TCPServer, extendedPacket_Association] := 
With[{
	uuid = extendedPacket["SourceSocket"][[1]], 
	expectedLength = extendedPacket["ExpectedLength"]
}, 
	If[KeyExistsQ[server["Buffer"], uuid] && server["Buffer", uuid]["Length"] > 0,  

		(*Return: _ByteArray*)
		Part[#, 1 ;; expectedLength]& @ 
		Apply[Join] @ 
		Append[extendedPacket["DataByteArray"]] @ 
		server["Buffer", uuid]["Elements"][[All, "DataByteArray"]], 

	(*Else*)

		(*Return: _ByteArray*)
		extendedPacket["DataByteArray"][[1 ;; expectedLength]]
	]
];  


TCPServer /: invokeHandler[server_TCPServer, packet_Association] := 
Module[{messageHandler, defaultMessageHandler}, 
	messageHandler = server["MessageHandler"]; 
	defaultMessageHandler = server["DefaultMessageHandler"]; 

	(*Return: ByteArray[] | _String | Null*)
	ConditionApply[messageHandler, defaultMessageHandler][packet]
]; 


TCPServer::cntsnd = 
"Can't send result to the client\n `1`"; 


TCPServer /: sendResponse[server_TCPServer, packet_Association, result: _ByteArray | _String | Null] := 
With[{client = packet["SourceSocket"]}, 
	Switch[result, 
		_String, 
			WriteString[client, result], 
		
		_ByteArray, 
			BinaryWrite[client, result], 

		Null, 
			Null
	]
]; 


TCPServer /: sendResponse[server_TCPServer, packet_Association, result_] := 
Message[TCPServer::cntsnd, result]; 


TCPServer /: savePacketToBuffer[server_TCPServer, extendedPacket_Association] := 
With[{uuid = extendedPacket["SourceSocket"]}, 
	If[KeyExistsQ[server["Buffer"], uuid], 
		server["Buffer", uuid]["Append", extendedPacket], 
		server["Buffer", uuid] = CreateDataStructure["DynamicArray", {extendedPacket}]
	]
]; 


TCPServer /: clearBuffer[server_TCPServer, packet_Association] := 
With[{uuid = packet["SourceSocket"]}, 
	If[KeyExistsQ[server["Buffer"], uuid], 
		server["Buffer", uuid]["DropAll"]; 
	]
]; 


(* ::Section::Closed:: *)
(*End private context*)


End[]; 


(* ::Section::Closed:: *)
(*End package*)


EndPackage[]; 
