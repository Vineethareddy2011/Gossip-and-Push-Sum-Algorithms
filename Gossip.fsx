module Gossip
#r "nuget: Akka.FSharp"
#r "nuget: Deedle"

open Akka
open Akka.FSharp
open Akka.Actor
open System

open System
open Deedle
open Akka.FSharp

// Round no of nodes to get perfect square in case of 2D and imperfect 3D grid
let roundNodes numNodes topology =
    match topology with
    | "2d" | "imperfect3d" -> 
        let cubeRoot = Math.Round(Math.Pow((float numNodes), 1.0 / 3.0))
        int (Math.Pow(cubeRoot, 3.0))
    | _ -> numNodes

// Select random element from a list
let random = Random()

let pickRandom (list: List<_>) =
    list.[random.Next(list.Length)]

let getRandomNeighborID (topologyMap: Map<_, List<_>>) nodeID =
    match topologyMap.TryFind nodeID with
    | Some neighborList -> pickRandom neighborList
    | None -> failwith "Node ID not found in the topology map."


//Different topologies


let buildLineTopology numNodes =
    [1 .. numNodes]
    |> List.map (fun nodeID ->
        let neighbors = 
            [nodeID - 1; nodeID + 1]
            |> List.filter (fun y -> y > 0 && y <= numNodes)
        nodeID, neighbors)
    |> Map.ofList

// Find neighbors of any particular node in a 2D grid
let gridNeighbors2D nodeID numNodes =
    let lenSide = int (Math.Sqrt (float numNodes))
    let isOnLeftEdge = nodeID % lenSide = 1
    let isOnRightEdge = nodeID % lenSide = 0
    let isOnTopEdge = nodeID <= lenSide
    let isOnBottomEdge = nodeID > numNodes - lenSide

    let left = if isOnLeftEdge then None else Some (nodeID - 1)
    let right = if isOnRightEdge then None else Some (nodeID + 1)
    let up = if isOnTopEdge then None else Some (nodeID - lenSide)
    let down = if isOnBottomEdge then None else Some (nodeID + lenSide)

    [left; right; up; down]
    |> List.choose id

let build2DTopology numNodes =
    [1 .. numNodes]
    |> List.map (fun nodeID -> nodeID, gridNeighbors2D nodeID numNodes)
    |> Map.ofList

// Find neighbors of any particular node in a 3D grid
let gridNeighbors3D nodeID numNodes =
    let lenSide = Math.Round(Math.Pow((float numNodes), (1.0 / 3.0))) |> int
    let layerSize = lenSide * lenSide
    let layer = (nodeID - 1) / layerSize
    let positionInLayer = (nodeID - 1) % layerSize

    let neighbors = 
        [nodeID - 1; nodeID + 1; nodeID - lenSide; nodeID + lenSide; nodeID - layerSize; nodeID + layerSize]
        |> List.filter (fun n -> 
            n > 0 && n <= numNodes && 
            Math.Abs((n - 1) / layerSize - layer) <= 1 && 
            Math.Abs((n - 1) % layerSize - positionInLayer) <= 1 + lenSide)

    neighbors

let buildImperfect3DTopology numNodes =
    let random = Random()
    [1 .. numNodes]
    |> List.map (fun nodeID ->
        let neighbors = gridNeighbors3D nodeID numNodes
        let randomNeighbor = 
            [1 .. numNodes]
            |> List.filter (fun n -> not (List.contains n neighbors) && n <> nodeID)
            |> fun l -> if l.Length > 0 then Some (l.[random.Next(l.Length)]) else None

        match randomNeighbor with
        | Some n -> nodeID, n :: neighbors
        | None -> nodeID, neighbors)
    |> Map.ofList

let buildFullTopology numNodes =
    [1 .. numNodes]
    |> List.map (fun nodeID ->
        let neighbors = [1 .. numNodes] |> List.filter (fun y -> y <> nodeID)
        nodeID, neighbors)
    |> Map.ofList


let buildTopology numNodes topology =
    let mutable map = Map.empty
    match topology with
    | "line" -> buildLineTopology numNodes
    | "2d" -> build2DTopology numNodes
    | "imperfect3d" -> buildImperfect3DTopology numNodes
    | "full" -> buildFullTopology numNodes
    | _ -> failwith "Unknown Topology! Make sure you carefully enter the topology."
//Actors
type CounterMessage =
    | GossipNodeConverge
    | PushSumNodeConverge of int * float

type Result = { NumberOfNodesConverged: int; TimeElapsed: int64; }

let counter initialCount numNodes (filepath: string) (stopWatch: Diagnostics.Stopwatch) (mailbox: Actor<'a>) =
    let rec loop count (dataframeList: Result list) =
        actor {
            let! message = mailbox.Receive()
            match message with
            | GossipNodeConverge ->
                let newRecord = { NumberOfNodesConverged = count + 1; TimeElapsed = stopWatch.ElapsedMilliseconds; }
                if (count + 1 = numNodes) then
                    stopWatch.Stop()
                    printfn "Gossip Algorithm has converged in %d ms" stopWatch.ElapsedMilliseconds
                    let dataframe = Frame.ofRecords dataframeList
                    dataframe.SaveCsv(filepath, separator=',')
                    mailbox.Context.System.Terminate() |> ignore
                return! loop (count + 1) (List.append dataframeList [newRecord])
            | PushSumNodeConverge (nodeID, avg) ->
                let newRecord = { NumberOfNodesConverged = count + 1; TimeElapsed = stopWatch.ElapsedMilliseconds }
                if (count + 1 = numNodes) then
                    stopWatch.Stop()
                    printfn "Push Sum Algorithm has converged in %d ms" stopWatch.ElapsedMilliseconds
                    let dataframe = Frame.ofRecords dataframeList
                    dataframe.SaveCsv(filepath, separator='\t')
                    mailbox.Context.System.Terminate() |> ignore
                return! loop (count + 1) (List.append dataframeList [newRecord])
        }
    loop initialCount []

//Gossip Algorithm

let gossip maxCount (topologyMap: Map<_, _>) nodeID counterRef (mailbox: Actor<_>) = 
    let rec loop (count: int) = actor {
        let! message = mailbox.Receive ()
        // Handle message here
        match message with
        | "heardRumor" ->
            // If the heard rumor count is zero, tell the counter that it has heard the rumor and start spreading it.
            // Else, increment the heard rumor count by 1
            if count = 0 then
                mailbox.Context.System.Scheduler.ScheduleTellOnce(
                    TimeSpan.FromMilliseconds(25.0),
                    mailbox.Self,
                    "spreadRumor"
                )
                // printfn "[INFO] Node %d has been converged" nodeID
                counterRef <! GossipNodeConverge
                return! loop (count + 1)
            else
                return! loop (count + 1)
        | "spreadRumor" ->
            // Stop spreading the rumor if has an actor heard the rumor atleast 10 times
            // Else, Select a random neighbor and send message "heardRumor"
            // Start scheduler to wake up at next time step
            if count >= maxCount then
                return! loop count
            else
                let neighborID = getRandomNeighborID topologyMap nodeID
                let neighborPath = @"akka://my-system/user/worker" + string neighborID
                let neighborRef = mailbox.Context.ActorSelection(neighborPath)
                neighborRef <! "heardRumor"
                mailbox.Context.System.Scheduler.ScheduleTellOnce(
                    TimeSpan.FromMilliseconds(25.0),
                    mailbox.Self,
                    "spreadRumor"
                )
                return! loop count
        | _ ->
            printfn " Node %d has received unhandled message" nodeID
            return! loop count
    }
    loop 0


//Pushsum Algorithm
 
type PushSumMessage =
    | Initialize
    | Message of float * float
    | Round

let pushSum (topologyMap: Map<_, _>) nodeID counterRef (mailbox: Actor<_>) = 
    let rec loop sNode wNode sSum wSum count isTransmitting = actor {
        if isTransmitting then
            let! message = mailbox.Receive ()
            match message with
            | Initialize ->
                mailbox.Self <! Message (float nodeID, 1.0)
                mailbox.Context.System.Scheduler.ScheduleTellRepeatedly (
                    TimeSpan.FromMilliseconds(0.0),
                    TimeSpan.FromMilliseconds(25.0),
                    mailbox.Self,
                    Round
                )
                return! loop (float nodeID) 1.0 0.0 0.0 0 isTransmitting
            | Message (s, w) ->
                return! loop sNode wNode (sSum + s) (wSum + w) count isTransmitting
            | Round ->
                // Select a random neighbor and send (s/2, w/2) to it
                let neighborID = getRandomNeighborID topologyMap nodeID
                let neighborPath = @"akka://my-system/user/worker" + string neighborID
                let neighborRef = mailbox.Context.ActorSelection(neighborPath)
                mailbox.Self <! Message (sSum / 2.0, wSum / 2.0)
                neighborRef <! Message (sSum / 2.0, wSum / 2.0)
                // Check convergence
                if(abs ((sSum / wSum) - (sNode / wNode)) < 1.0e-10) then
                    let newCount = count + 1
                    if newCount = 10 then
                        counterRef <! PushSumNodeConverge (nodeID, sSum / wSum)
                        return! loop sSum wSum 0.0 0.0 newCount false
                    else
                        return! loop (sSum / 2.0) (wSum / 2.0) 0.0 0.0 newCount isTransmitting 
                else
                    return! loop (sSum / 2.0) (wSum / 2.0) 0.0 0.0 0 isTransmitting
    }
    loop (float nodeID) 1.0 0.0 0.0 0 true

