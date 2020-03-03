
SELECT
	L.Gid, 
	KVV.StringValue,
  DOL.ExistsFromDate,
  DOL.ExistsUptoDate
	FROM [ptDOI4_Community].[dbo].[Line] AS L
    JOIN [ptDOI4_Community].[dbo].[DirectionOfLine] AS DOL ON DOL.IsOnLineId = L.Id
    JOIN [ptDOI4_Community].[dbo].[VehicleJourneyTemplate] AS VJT ON VJT.IsWorkedOnDirectionOfLineGid = DOL.Gid
    JOIN [ptDOI4_Community].[dbo].[VehicleJourney] AS VJ ON VJ.IsDescribedByVehicleJourneyTemplateId = VJT.Id
    JOIN [ptDOI4_Community].[T].[KeyVariantValue] AS KVV ON KVV.IsForObjectId = VJ.Id
    JOIN [ptDOI4_Community].[dbo].[KeyVariantType] AS KVT ON KVT.Id = KVV.IsOfKeyVariantTypeId
    JOIN [ptDOI4_Community].[dbo].[KeyType] AS KT ON KT.Id = KVT.IsForKeyTypeId 
    JOIN [ptDOI4_Community].[dbo].[ObjectType] AS OT ON OT.Number = KT.ExtendsObjectTypeNumber

	WHERE KT.Name = 'RouteName'

	GROUP BY L.Gid, KVV.StringValue, DOL.ExistsFromDate, DOL.ExistsUptoDate
	ORDER BY L.Gid DESC;
